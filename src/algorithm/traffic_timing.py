import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
import math
import datetime
import time
import xml.etree.cElementTree as xee


class TrafficTiming:
    """
    主要流程：
        - 加载配置文件，读取车流率数据
        - 计算关键相位车流率信息
        - 韦伯斯特模型计算配时信息
        - 对早晚高峰配时时段进行聚类细分
        - 以早晚高峰方案数限制和最小切换间隔修正配时方案
        - 以各聚类内箱线图间相位时间均值设置相应时段配时
        - 校核周期及绿灯时间，并根据行人最小绿灯时间调整
        - 预估车流通行量并输出最终配时方案
    """

    def __init__(self, vehicle_flow_rate, traffic_light_file, inter_id, plan_para):
        '''
        两个关键参数：
            - 配置文件加载——config.json
            - 关键相位车流率——key_phase_flow_rate.csv
        '''
        # self.config = config
        self.vehicle_flow_rate = vehicle_flow_rate
        self.traffic_light_file = traffic_light_file
        self.plan_para = plan_para
        self.inter_id = inter_id
        self.flow_phase_para = None  # 关键相位信息
        self.phase_stage_infos = None  # ring-phase与stage-phase在各day_no,period_no下的对应关系
        self.timing = None  # 最终配时方案
        self.time_out = None  # 配时输出方案
        self.all_red = 0
        self.start_loss = 3
        self.time_loss = None
        self.scene_quantile = 0.9

    def get_key_phase_flow_rate(self):
        """
        获取关键相位车流率:区分路口、工作日和周末、分时段统计关键相位车流量
        """

        self.vehicle_flow_rate['sat_flow'] = self.vehicle_flow_rate.apply(
            lambda x: float(x['sat_flow']) if x['sat_flow'] != '' else 0.0, axis=1)
        self.vehicle_flow_rate = self.vehicle_flow_rate.query('sat_flow != 0.0')
        # 保存ring-phase与stage-phase间不同时段下的映射关系
        self.phase_stage_infos = self.vehicle_flow_rate.groupby(['day_no', 'period_no', 'phase', 'stage_no']).agg(
            {'links_no': 'count'}).reset_index()

        vehicle_flow_temp = self.vehicle_flow_rate
        vehicle_flow_temp['yi'] = vehicle_flow_temp['flow_rate'] / vehicle_flow_temp['sat_flow']  # 流率比

        vehicle_flow_temp = vehicle_flow_temp.groupby(
            ['day_no', 'date', 'period_no', 'plan_no', 'time', 'stage_no', 'min_cycle', 'max_cycle']).agg(
            {'all_red': max, 'yellow': max, 'min_green': max, 'yi': max}).reset_index()  # 取同一时段，同一相位的最大yi值
        self.flow_phase_para = vehicle_flow_temp.groupby(['day_no', 'period_no', 'plan_no', 'time', 'stage_no']).agg(
            {'all_red': max, 'yellow': max, 'min_green': max}).reset_index()
        self.flow_phase_para['all_red'] = self.flow_phase_para['all_red'].astype('int')

        vehicle_flow_temp['Yr'] = \
            vehicle_flow_temp.groupby(['day_no', 'date', 'period_no', 'plan_no', 'time', 'min_cycle', 'max_cycle'])[
                'yi'].transform('sum')
        vehicle_flow_unstack = vehicle_flow_temp.set_index(
            ['day_no', 'date', 'period_no', 'plan_no', 'time', 'min_cycle', 'max_cycle', 'Yr', 'stage_no'])[
            'yi'].unstack().reset_index()

        def Yr_quantile(sr):  # 按分位数，返回交叉口关键相位的总饱和度值
            down = sr.quantile(self.scene_quantile)
            return sr[(sr >= down)].min()

        vehicle_flow_unstack['Yr_quantile'] = \
            vehicle_flow_unstack.groupby(['day_no', 'period_no', 'plan_no', 'time', 'min_cycle', 'max_cycle'])[
                'Yr'].transform(Yr_quantile)
        vehicle_flow_unstack = vehicle_flow_unstack[
                                   vehicle_flow_unstack['Yr'] >= vehicle_flow_unstack['Yr_quantile']].iloc[:, :-1]

        key_phase_flow_stack = vehicle_flow_unstack.groupby(
            ['day_no', 'period_no', 'plan_no', 'time', 'min_cycle', 'max_cycle']).agg(
            'mean').reset_index()  # 选择某一饱和度区间对应的相位流率均值作为算法输入
        key_phase_flow = key_phase_flow_stack.set_index(
            ['day_no', 'period_no', 'plan_no', 'time', 'min_cycle', 'max_cycle', 'Yr']).stack().rename(
            'yi').reset_index().rename(columns={'level_4': 'stage_no'})
        self.vehicle_flow_rate = key_phase_flow
        return self

    def webster_timing(self):
        """
        依据webster模型进行配时方案计算，计算粒度：6min
            1. T = (1.5L + 5) / (1 - Y)
                - L = sigma(li) = n_phase * (start_loss + all_red)
                - Y = sigma(yi) = sigma(key_flow_rate / sat_flow_rate)
            2. gi = (T - L) * yi / Y - start_loss + yellow
        """
        self.start_loss = 3
        flow_loss_time = self.flow_phase_para.groupby(['day_no', 'period_no', 'plan_no', 'time']).agg(
            {'all_red': sum}).rename(columns={'all_red': 'all_red_lost'}).reset_index()  # 周期内全红时间计算

        flow_rate = self.vehicle_flow_rate.copy()
        # flow_rate['Yr'] = flow_rate.groupby(['day_no','period_no','plan_no','time','min_cycle','max_cycle','all_red','yellow','min_green'])['yi'].transform('sum')  # 实际的yi求和Yreal
        flow_rate['Yc'] = flow_rate['Yr'].map(lambda x: min(0.9, max(0.1, x)))  # 计算周期的yi之和Ycount
        # 求解相位个数
        flow_rate['n_phase'] = flow_rate.groupby(['day_no', 'period_no', 'plan_no', 'min_cycle', 'max_cycle', 'time'])[
            'yi'].transform('count')
        flow_rate = pd.merge(flow_rate, self.flow_phase_para, on=['day_no', 'period_no', 'plan_no', 'time', 'stage_no'],
                             how='left')  # 添加全红，最小绿，黄灯时间
        flow_rate = pd.merge(flow_rate, flow_loss_time, on=['day_no', 'period_no', 'plan_no', 'time'],
                             how='left')  # 添加周期内的全红时间
        # webster模型求解最佳周期
        flow_rate['T'] = flow_rate.apply(
            lambda x: (1.5 * (x['all_red_lost'] + self.start_loss * x['n_phase']) + 5) / (1 - x['Yc']), axis=1)

        flow_rate['max_cycle'] = flow_rate['max_cycle'].astype('int')
        flow_rate['min_cycle'] = flow_rate['min_cycle'].astype('int')
        flow_rate['min_green'] = flow_rate['min_green'].astype('int')
        flow_rate['yellow'] = flow_rate['yellow'].astype('int')

        def get_Tmm(sr):  # 根据交叉口名称，时段
            return min(sr['max_cycle'], max(sr['min_cycle'], sr['T']))

        # 求解各相位显示绿灯时间:有效绿灯时间 * 流率比权重 + 启动损失时间 - 黄灯时间
        def get_green(sr):
            if sr['Yr'] > 0:  # 流率比之和不为0，加权占比求解有效
                return sr['yi'] / sr['Yr'] * (
                            sr['Tmm'] - (sr['all_red_lost'] + self.start_loss * sr['n_phase'])) + self.start_loss - sr[
                           'yellow']
            else:  # 调用其相位最小绿灯时间
                return sr['min_green']

        def judge_min_green(sr):  # 判断当前绿灯是否小于最小绿灯时间
            if 'less_pedestrian_time' in flow_rate.columns.tolist():
                if sr['less_pedestrian_time']:
                    return sr['less_pedestrian_time']
            return sr['phase_time'] < sr['min_green']

        flow_rate['Tmm'] = flow_rate.apply(get_Tmm, axis=1)
        # 求解各相位显示绿灯时间:有效绿灯时间 * 流率比权重 + 启动损失时间 - 黄灯时间

        flow_rate['phase_time'] = flow_rate.apply(get_green, axis=1)

        def get_effective_green(sr):
            if sr['less_pedestrian_time']:
                sr['phase_time'] = sr['min_green']
            else:
                if sr['effective_sum_yi'] > 0:  # 流率比之和不为0，加权占比求解有效
                    sr['phase_time'] = math.floor(sr['yi'] / sr['effective_sum_yi'] * (
                            sr['Tmm'] - sr['sum_min_time'] - (
                                sr['all_red_lost'] + self.start_loss * sr['n_phase'])) + self.start_loss - sr[
                                                     'yellow'])  # 向上取整
                else:  # 调用其相位最小绿灯时间
                    sr['phase_time'] = sr[
                        'min_green']  # self.config['road_settings'][sr['inter_name']]['min_green'][sr['phase']]
            return sr

        # 调整非最小绿相位的绿灯分配方法
        # 建立循环，直到不存在小于最小绿的相位
        i = 0
        while len(flow_rate[flow_rate['phase_time'] < flow_rate['min_green']]) > 0 or i < flow_rate['n_phase'].max():
            flow_rate['less_pedestrian_time'] = flow_rate.apply(judge_min_green, axis=1)
            sum_green = flow_rate.groupby(['day_no', 'period_no', 'plan_no', 'time']).apply(
                lambda x: (x[x['less_pedestrian_time']]['min_green']).sum()).rename('sum_min_time').reset_index()
            sum_yi = flow_rate.groupby(['day_no', 'period_no', 'plan_no', 'time']).apply(
                lambda x: x[x['less_pedestrian_time'] == False]['yi'].sum()).rename('effective_sum_yi').reset_index()
            flow_rate_temp = pd.merge(sum_green, sum_yi, on=['day_no', 'period_no', 'plan_no', 'time'], how='inner')
            # flow_rate与flow_rate_temp进行左连接并赋值给flow_rate
            flow_rate = pd.merge(flow_rate, flow_rate_temp, on=['day_no', 'period_no', 'plan_no', 'time'], how='left')
            flow_rate = flow_rate.apply(get_effective_green, axis=1)
            flow_rate = flow_rate.drop(columns=['sum_min_time', 'effective_sum_yi'], axis=1)
            i += 1

        # flow_rate['phase_time'] += (flow_rate['all_red'] + flow_rate['yellow'])  # 补全全红时间和黄灯时间
        flow_rate['green_ratio'] = flow_rate['phase_time'] / flow_rate['Tmm']
        flow_rate['flow_ratio'] = flow_rate['yi'] / flow_rate['Yr']

        # 根据四舍五入后的相位时间重新计算周期
        self.timing = flow_rate[
            ['day_no', 'period_no', 'plan_no', 'time', 'stage_no', 'phase_time', 'all_red', 'yellow', 'n_phase', 'yi',
             'Yr', 'T', 'Tmm', 'green_ratio', 'flow_ratio']]  # 暂不保留周期数据
        return self

    def timing_cluster(self):
        """
        获取聚类标签
        """
        timing = self.timing.reset_index(drop=True)
        # 增加车流变化率特征
        for _, group in timing.groupby(['day_no', 'period_no', 'plan_no', 'stage_no']):
            group.sort_values(by='time', inplace=True)
            timing.loc[group.index, 'slope'] = group['yi'].diff() / group['yi'].shift(1)

        timing.fillna(method='bfill', inplace=True)

        # 对峰值时段内的时隙进行聚类
        timing['cluster_label'] = -1  # 平峰聚类标签统一设置为-1
        for name, group in timing.groupby(['day_no', 'period_no', 'plan_no']):
            # if 'flat' in name[-1]:
            # continue
            X = group.pivot(index='time', columns='stage_no', values=['slope', 'phase_time']).sort_index()
            X['n'] = np.arange(len(X)) * 6
            X.replace([np.inf, -np.inf], 0, inplace=True)
            X.fillna(0, inplace=True)
            time_label_dict = self._cluster(X)
            timing.loc[group.index, 'cluster_label'] = group.time.map(time_label_dict)

        self.timing = timing.astype({'cluster_label': int}).drop(columns='slope')
        return self

    def _cluster(self, X):
        """
        对高峰期配时时隙进行层次聚类
        """
        peak_max_plans = 1  # self.config['general_settings']['peak_max_plans']
        peak_min_interval = 30  # self.config['general_settings']['peak_min_interval']
        min_labels = math.ceil(peak_min_interval / 6)  # 最小时间间隔转化为对最小连续标签数的限制
        labels = AgglomerativeClustering(peak_max_plans).fit_predict(X).tolist()
        # 依据调整最小时间间隔调整聚类标签
        n = len(labels)
        if n > 1:
            labels[1] = labels[0]
        for i in range(1, n):
            combine = True
            for j in range(i + 1 - min_labels, i + 1):
                if labels[j:j + min_labels].count(labels[i]) == min_labels:
                    # 存在连续相同的min_labels个相同标签，意味着无需合并
                    combine = False
                    break
            if combine:
                labels[i] = labels[i - 1]
        return dict(zip(X.index, pd.Series(labels)))

    def get_phase_time(self):
        """
        主要功能：
            - 获取各时段-聚类内平均相位时间作为该聚类的配时数据
            - 整合配时方案——剔除重复值
        """

        def green_mean_quantile(sr):
            delta_q = sr.quantile(0.75) - sr.quantile(0.25)
            down = sr.quantile(0.5) - 3 * delta_q
            up = sr.quantile(0.5) + 3 * delta_q
            return sr[(sr <= up) & (sr >= down)].mean()

        def yellow_red_mean_quantile(sr):
            delta_q = sr.quantile(0.75) - sr.quantile(0.25)
            down = sr.quantile(0.5) - 3 * delta_q
            up = sr.quantile(0.5) + 3 * delta_q
            return sr[(sr <= up) & (sr >= down)].max()

        timing = self.timing
        timing['green'] = timing.groupby(['stage_no', 'day_no', 'period_no', 'plan_no', 'cluster_label'])[
            'phase_time'].transform(green_mean_quantile).astype(int)  # 绿灯时长直接取整，.apply(lambda x:math.ceil(x))
        timing['yellow'] = timing.groupby(['stage_no', 'day_no', 'period_no', 'plan_no', 'cluster_label'])[
            'yellow'].transform(yellow_red_mean_quantile).astype(int)
        timing['all_red'] = timing.groupby(['stage_no', 'day_no', 'period_no', 'plan_no', 'cluster_label'])[
            'all_red'].transform(yellow_red_mean_quantile).astype(int)
        # timing.drop_duplicates(subset=['inter_name', 'phase', 'week_label', 'peak_type', 'cluster_label'], inplace=True)
        timing['green_ratio'] = timing.groupby(['stage_no', 'day_no', 'period_no', 'plan_no', 'cluster_label'])[
            'green_ratio'].transform(green_mean_quantile)
        timing['flow_ratio'] = timing.groupby(['stage_no', 'day_no', 'period_no', 'plan_no', 'cluster_label'])[
            'flow_ratio'].transform(green_mean_quantile)
        self.timing = timing.reset_index(drop=True)
        return self

    def get_result(self):
        self.timing = self.timing[['day_no', 'period_no', 'plan_no', 'time', 'stage_no', 'green', 'yellow', 'all_red']]
        #         self.timing = self.timing[['stage_no', 'day_no','period_no', 'time', 'phase_time', 'n_phase','green_ratio','flow_ratio']]
        #         self.time_out = self.timing.set_index(['day_no','period_no', 'time', 'stage_no'])['phase_time'].unstack().reset_index()
        #         self.time_out = self.time_out.rename(columns={1:'stage1',2:'stage2',3:'stage3'})
        #         green_ratio_out = self.timing.set_index(['day_no','period_no', 'time', 'stage_no'])['green_ratio'].unstack().reset_index()
        #         green_ratio_out = green_ratio_out.rename(columns={1:'green_ratio1',2:'green_ratio2',3:'green_ratio3'})
        #         flow_ratio_out = self.timing.set_index(['day_no','period_no', 'time', 'stage_no'])['flow_ratio'].unstack().reset_index()
        #         flow_ratio_out = flow_ratio_out.rename(columns={1:'flow_ratio1',2:'flow_ratio2',3:'flow_ratio3'})
        #         self.time_out.loc[:, 'T'] = self.time_out.sum(axis=1)

        #         #self.time_out分别与green_ratio_out,flow_ratio_out进行左连接，拼接到self.time_out上
        #         self.time_out = pd.merge(self.time_out,green_ratio_out,on=['day_no','period_no', 'time'],how = 'left')
        #         self.time_out = pd.merge(self.time_out,flow_ratio_out,on=['day_no','period_no', 'time'],how = 'left')

        subcolumns = self.timing.columns.tolist()
        subcolumns.remove('time')
        self.time_out = self.timing.sort_values(['day_no', 'period_no', 'time']).drop_duplicates(
            subcolumns).reset_index(drop=True)
        self.time_out['phase_time'] = self.time_out['green'] + self.time_out['yellow'] + self.time_out['all_red']
        self.time_out['cycle'] = self.time_out.groupby(['day_no', 'period_no', 'plan_no'])[
            'phase_time'].transform('sum')
        return self

    def auto_timing(self):
        print('2、开始进行配时分析……')
        self.get_key_phase_flow_rate()
        print('- 获取关键相位车流率完成')
        self.webster_timing()
        print('- webster初配时完成')
        self.timing_cluster()
        # print('- 峰值时段聚类完成')
        self.get_phase_time()
        print('- 聚类配时完成')
        self.get_result()
        print('- 配时输出方案完成')

    def return_phase_plan(self):
        """
        返回阶段字典值
        """
        for i in range(0, len(self.time_out)):
            plan_no = self.time_out.iloc[i]['plan_no']
            cycle = self.time_out.iloc[i]['cycle']
            stage_id = self.time_out.iloc[i]['stage_no']
            stage_index = int(stage_id.split('P')[1]) - 1
            self.plan_para['phase_plan'][stage_index]['green_time'] = int(self.time_out.iloc[i]['phase_time'])
            self.plan_para['phase_plan'][stage_index]['yellow'] = int(self.time_out.iloc[i]['yellow'])
            self.plan_para['phase_plan'][stage_index]['all_red'] = int(self.time_out.iloc[i]['all_red'])
        return pd.Series([plan_no, cycle, self.plan_para])

    def write_state_xml(self):  # 相序和相位不变下，微调时长
        domTree = xee.parse(self.traffic_light_file)
        rootNode = domTree.getroot()
        lights = rootNode.findall('light')
        for light in lights:
            print('id:', light.get('id'))
            if (light.get('id') == self.inter_id):
                plans = light.findall('plan')
                plans_from_xml = {}
                for plan in plans:
                    plan_no = plan.get('no')
                    plans_from_xml[plan_no] = plan

                system_time_text = light.findall('system_time')
                system_time_text[0].text = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                plans_from_algorithm = self.time_out.drop_duplicates('plan_no')['plan_no']
                for i in range(0, len(plans_from_algorithm)):
                    plan_no = plans_from_algorithm.iloc[i]
                    if plan_no in plans_from_xml.keys():
                        cycle_text = plans_from_xml[plan_no].findall('cycle')
                        cycle = self.time_out[self.time_out['plan_no'] == plan_no]['cycle'].iloc[0]
                        # 修改元素的内容
                        cycle_text[0].text = str(cycle)

                        # 解析XML中的主相位编号
                        rings = plans_from_xml[plan_no].findall('ring')
                        for ring in rings:
                            states = ring.findall('state')
                            for state in states:
                                phase_no = state.get('phase')
                                # 在phase_stage_infos中找到对应的阶段
                                stage_no = \
                                self.phase_stage_infos[self.phase_stage_infos['phase'] == phase_no]['stage_no'].iloc[0]
                                # 在time.time_out中提取出绿灯、黄灯和全红时间
                                green = self.time_out[
                                    (self.time_out['plan_no'] == plan_no) & (self.time_out['stage_no'] == stage_no)][
                                    'green'].iloc[0]
                                yellow = self.time_out[
                                    (self.time_out['plan_no'] == plan_no) & (self.time_out['stage_no'] == stage_no)][
                                    'yellow'].iloc[0]
                                all_red = self.time_out[
                                    (self.time_out['plan_no'] == plan_no) & (self.time_out['stage_no'] == stage_no)][
                                    'all_red'].iloc[0]
                                # 修改元素的附加属性（键值对）
                                state.set('green', str(green))
                                state.set('yellow', str(yellow))
                                state.set('all_red', str(all_red))

        domTree.write("2信控-new.xml", encoding='utf-8')