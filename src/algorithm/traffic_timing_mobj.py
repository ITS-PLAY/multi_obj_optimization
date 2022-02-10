import pandas as pd
import numpy as np
import math
import sys
import copy

from queue import PriorityQueue
import xml.etree.cElementTree as xee
from collections import Counter
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score

class TrafficTimingMultiObject:
    def __init__(self,vehicle_flow_rate, traffic_light_file, inter_id, plan_para, phase_lane, flow_interval=6):

        self.vehicle_flow_rate = vehicle_flow_rate
        self.phase_lane = phase_lane
        self.traffic_light_file = traffic_light_file
        self.inter_id = inter_id
        self.plan_para = plan_para
        self.flow_interval = flow_interval

        self.step = self.plan_para['step']  # 搜索步长
        self.start_loss = 3.0              # 启动损失时间
        self.stop_density = 1000 / 9.0     # 排队状态下的拥堵密度
        self.sat_ratio_threshold = 0.8     # 判断拥堵状态的阈值
        self.ring_num = 2                 # 环数
        self.scene_quantile = 0.9         # 场景覆盖率

        self.timing = pd.DataFrame()  # 最终配时方案
        self.time_out = pd.DataFrame()  # 配时输出方案

    # 复用现有关键车流选取方法，选取>0.9的场景流量，作为车道流量输入(未采用）；以进口道、相位和车道作为分组依据，取同一进口道、相同相位的车道流量最大，作为相位的关键输入；
    def get_key_phase_flow_rate(self):
        """
        获取关键相位车流率:分时段统计关键相位车流量
        """

        self.vehicle_flow_rate['sat_flow'] = self.vehicle_flow_rate.apply(
            lambda x: float(x['sat_flow']) if x['sat_flow'] != '' else 0.0, axis=1)
        self.vehicle_flow_rate = self.vehicle_flow_rate.query('sat_flow != 0.0')

        self.vehicle_flow_rate['yi'] = self.vehicle_flow_rate['flow_rate'] / self.vehicle_flow_rate['sat_flow']  # 流率比

        self.vehicle_flow_rate['Yr'] = \
            self.vehicle_flow_rate.groupby(['day_no', 'date', 'period_no', 'plan_no', 'time'])['yi'].transform('sum')

        def Yr_quantile(sr):  # 按分位数，返回交叉口关键相位的总饱和度值
            down = sr.quantile(self.scene_quantile)
            return sr[(sr >= down)].min()

        self.vehicle_flow_rate['Yr_quantile'] = \
            self.vehicle_flow_rate.groupby(['day_no', 'period_no', 'plan_no', 'time'])['Yr'].transform(Yr_quantile)
        self.vehicle_flow_rate = self.vehicle_flow_rate[
                                self.vehicle_flow_rate['Yr'] >= self.vehicle_flow_rate['Yr_quantile']].iloc[:, :-1]

        self.vehicle_flow_rate = self.vehicle_flow_rate.groupby(
            ['day_no', 'period_no', 'plan_no', 'time', 'road', 'phase', 'lane']).agg(
            {'flow_rate': 'mean'}).reset_index()  # 选择某一饱和度区间对应的相位流率均值作为算法输入

        key_phase_flow = self.vehicle_flow_rate.groupby(['day_no', 'period_no', 'plan_no', 'time', 'road', 'phase']).agg(
            {'flow_rate': 'max'}).reset_index()  # 取同一时段，同一相位的最大yi值
        key_phase_flow['road_flow_rate'] = key_phase_flow.groupby(['day_no', 'period_no', 'plan_no', 'time', 'road'])[
            'flow_rate'].transform('mean')
        key_phase_flow = key_phase_flow.query('phase != ""')
        return key_phase_flow

    def list_to_str(self,detect_ids):
        id_string = ''
        for j in range(0, len(detect_ids)):
            id_string += "%s" % detect_ids[j] + ','
        id_string = id_string[:-1]
        return id_string

    # 将输入阶段转为环结构，并判断相位是否存在搭接和屏障；通过在双环的相位上，添加搭接标志或者屏障，选择每个阶段的最大或最小流量作为阶段的流量。
    def judge_main_phase_overlap(self):
        ring_list = [[] for i in range(0, self.ring_num)]

        ring_col_index = {}
        for i in range(0, self.ring_num):
            ring_col_index[i] = -1

        overlap_phases = {}  # {key:value}=>{overlap:main_phase}
        overlap_phases_num = {}  # {key:value}=>{overlap:the num of overlap main phase}
        overlap = {}  # {key:value}=>{main_phase:overlap list}

        def list_to_str(detect_ids):
            id_string = ''
            for j in range(0, len(detect_ids)):
                id_string += "%s" % detect_ids[j] + ','
            id_string = id_string[:-1]
            return id_string

        phase_plan = self.plan_para['phase_plan']
        for i in range(0, len(phase_plan)):
            stage_info = phase_plan[i]['id']
            # 提取阶段里的所有相位
            ring_phases_no = [[] for i in range(0, self.ring_num)]
            for j in range(0, len(stage_info)):
                if stage_info[j] in ('1', '2', '3', '4', '5', '6', '7', '8'):
                    if stage_info[j] not in overlap_phases_num.keys():
                        overlap_phases_num[stage_info[j]] = 0
                    else:
                        overlap_phases_num[stage_info[j]] += 1
                if stage_info[j] in ('1', '2', '3', '4'):
                    ring_phases_no[0].append(stage_info[j])
                elif stage_info[j] in ('5', '6', '7', '8'):
                    ring_phases_no[1].append(stage_info[j])

            for m in range(0, self.ring_num):
                if len(ring_phases_no[m]) == 0:
                    continue

                # 存储主相位的编号
                ring_main_phase = 0
                if len(ring_phases_no[m]) == 1:
                    ring_main_phase = ring_phases_no[m][0]
                else:
                    # 按照直行、左转的顺序确定主相位
                    straight_list = [no for no in ring_phases_no[m] if int(no) % 2 == 0]
                    left_list = [no for no in ring_phases_no[m] if int(no) % 2 == 1]
                    if len(straight_list) > 0:
                        ring_main_phase = straight_list[0]
                    elif len(left_list) > 0:
                        ring_main_phase = left_list[0]
                    else:
                        ring_main_phase = ring_phases_no[m][0]

                    ring_phases_no[m].remove(ring_main_phase)
                    for overlap_p in ring_phases_no[m]:
                        if overlap_p in overlap_phases.keys():
                            overlap_phases[overlap_p].append(ring_main_phase)
                        else:
                            overlap_phases[overlap_p] = [ring_main_phase]

                # 按照主相位，添加屏障和搭接的默认字段，建立环列表
                dict_info = {'id': ring_main_phase, 'overlap': '', 'is_overlap': '0', 'barrier': '0'}
                ring_list[m].append(dict_info)
                ring_col_index[m] += 1

            # 更新相位后的屏障
            flag = False
            barrier_condition = [[('1', '2'), ('3', '4')],
                                 [('5', '6'), ('7', '8')]]

            for m in range(0, self.ring_num):
                if (len(ring_list[m]) == 0) or (len(ring_list[m]) == 1):
                    continue
                length = len(barrier_condition[m])
                for b in range(0, length):
                    if (ring_list[m][ring_col_index[m] - 1]['id'] in barrier_condition[m][b]) and (
                            ring_list[m][ring_col_index[m]]['id'] in barrier_condition[m][length - 1 - b]):
                        flag = True
            # 当数组第一行的相位编号由1（或者2）互换为3（或者4），或者数组第二行的相位编号由5（或者6）互换为7（或者8）时，同时更新第一行和第二行的前一个元素的相位后屏障值；
            if flag:
                for m in range(0, self.ring_num):
                    if len(ring_list[m]) == 1:
                        dict_info = {'id': '0', 'overlap': '', 'is_overlap': '0', 'barrier': '1'}
                        ring_list[m].insert(0, dict_info)
                        ring_col_index[m] += 1
                    else:
                        ring_list[m][ring_col_index[m] - 1]['barrier'] = '1'

            # 或者遍历结束时，同时更新第一行和第二行的前一个元素的相位后屏障值；
            if i == len(phase_plan) - 1:
                for m in range(0, self.ring_num):
                    ring_list[m][ring_col_index[m]]['barrier'] = '1'

        # 添加搭接相位
        for key, value in overlap_phases.items():
            if len(value) == 1:
                value = value[0]
                if value not in overlap.keys():
                    overlap[value] = [key]
                else:
                    overlap[value].append(key)
            else:
                # 如果值集合中有元素出现多次，将该元素作为主相位
                value_count = Counter(value)
                max_value = 1
                main_phase = ''
                for k, value in value_count.items():
                    if value > max_value:
                        main_phase = k
                        max_value = value
                if max_value > 1:
                    if main_phase not in overlap.keys():
                        overlap[main_phase] = [key]
                    else:
                        overlap[main_phase].append(key)
                    continue
        # 添加主相位是否搭接相位的标志
        for m in range(0, self.ring_num):
            for n in range(0, len(ring_list[m])):
                ring_main_phase = ring_list[m][n]['id']
                if ring_main_phase == '0':
                    continue
                if ring_main_phase in overlap.keys():
                    ring_list[m][n]['overlap'] = list_to_str(overlap[ring_list[m][n]['id']])
                if overlap_phases_num[ring_main_phase] > 0:
                    ring_list[m][n]['is_overlap'] = '1'
                    overlap_phases_num[ring_main_phase] -= 1
        return ring_list

    def algorithm_input(self, key_phase_flow, time):
        flow = key_phase_flow[key_phase_flow['time'] == time]
        return flow

    # 将输入阶段转为环结构，并判断相位是否存在搭接和屏障；
    # 通过在双环的相位上，添加搭接标志或者屏障;选择每个阶段的最大或最小流量作为阶段的流量。
    ##主相位为屏障相位，阶段取最大流量；如果双环存在至少一个搭接主相位，则阶段选取最小流量；如果双环不存在搭接相位，则阶段选取最大流量。
    # 根据阶段的流量，计算主相位的排队长度；每个阶段添加个清空系数，用于在周期一定下，调整某一相位的绿灯时长。
    def return_flow_input(self, ring_list, flow):
        flow_dict = {}
        for i in range(0, self.ring_num):
            for j in range(0, len(ring_list[i])):
                flow_dict[ring_list[i][j]['id']] = 0

        for i in range(0, len(flow)):
            flow_dict[flow['phase'].iloc[i]] = flow['cycle_flow'].iloc[i]
        return flow_dict

    def caculate_stage_flow(self, flow_input, ring_overlap, phase_plan):

        ring_col_index = {}
        for i in range(0, self.ring_num):
            ring_col_index[i] = 0

        def judge_termination_condition(ring_col_index, ring_list):
            for i in range(0, self.ring_num):
                if ring_col_index[i] != len(ring_list[i]):
                    return True

        # 计算初次的阶段流量和绿灯分配结果
        stage_i = 0
        while judge_termination_condition(ring_col_index, ring_overlap):

            max_min_calibration_num = 0
            for i in range(0, self.ring_num):
                if ring_col_index[i] == len(ring_overlap[i]):
                    ring_col_index[i] -= 1
                    continue
                if ring_overlap[i][ring_col_index[i]]['is_overlap'] == '1':
                    max_min_calibration_num = 1

            flow_opt_criterion = 'max'

            if max_min_calibration_num == 1:
                flow_opt_criterion = 'min'

            ring1_phase = ring_overlap[0][ring_col_index[0]]['id']
            ring2_phase = ring_overlap[1][ring_col_index[1]]['id']

            ##如果双环存在至少一个搭接主相位，则阶段选取最小流量；如果双环不存在搭接相位，则阶段选取最大流量;主相位为屏障相位，阶段取最大流量
            stage_key_phase = ""

            def caculate_phase_max_flow(overlap, flow_input, ring_phase):
                ring_max_flow = flow_input[ring_phase]
                if overlap != '':
                    ring_list = overlap.split(',')
                    for ring_p in ring_list:
                        if ring_p not in flow_input.keys():
                            continue
                        else:
                            ring_max_flow = max(ring_max_flow, flow_input[ring_p])
                return ring_max_flow

            ring1_max_flow = caculate_phase_max_flow(ring_overlap[0][ring_col_index[0]]['overlap'], flow_input,
                                                     ring1_phase)
            ring2_max_flow = caculate_phase_max_flow(ring_overlap[1][ring_col_index[1]]['overlap'], flow_input,
                                                     ring2_phase)

            if flow_opt_criterion == 'max':
                stage_flow = max(ring1_max_flow, ring2_max_flow)
            else:
                stage_flow = min(ring1_max_flow, ring2_max_flow)

            ##将阶段流量stage_flow、关键相位stage_key_phase、清空系数stage_clear_ratio、阶段时长stage_time添加到phase_plan中
            if stage_flow == ring1_max_flow:
                stage_key_phase = ring1_phase
            else:
                stage_key_phase = ring2_phase

            for ring_i in range(0, self.ring_num):
                overlap = ring_overlap[ring_i][ring_col_index[ring_i]]['overlap']
                if overlap != '':
                    ring_list = overlap.split(',')
                    for ring_p in ring_list:
                        if ring_p not in flow_input.keys():
                            continue
                        else:
                            flow_input[ring_p] -= stage_flow

            flow_input[ring1_phase] -= stage_flow
            flow_input[ring2_phase] -= stage_flow

            phase_plan[stage_i]['stage_flow'] = stage_flow
            phase_plan[stage_i]['stage_key_phase'] = stage_key_phase
            phase_plan[stage_i]['stage_clear_ratio'] = 1.0

            if ring_overlap[0][ring_col_index[0]]['barrier'] == ring_overlap[1][ring_col_index[1]]['barrier']:
                for i in range(0, self.ring_num):
                    ring_col_index[i] += 1
            else:
                for i in range(0, self.ring_num):
                    if ring_overlap[i][ring_col_index[i]]['barrier'] == '0':
                        ring_col_index[i] += 1

            stage_i += 1

        ##将阶段的流量和阶段关键相位的通行能力，作为阶段配时的依据（阶段配时取阶段内相位绿灯最长的）；
        for i in range(0, len(phase_plan)):
            key_phase = phase_plan[i]['stage_key_phase']
            for _, values in self.phase_lane.items():
                for _, value in values.items():
                    if value['phase'] == key_phase:
                        phase_plan[i]['sat_headway_time'] = 3600 / float(value['sat_flow'])
                        min_stage_time = max(phase_plan[i]['min_green'], phase_plan[i]['pedestrian_time']) + \
                                         phase_plan[i][ \
                                             'yellow'] + phase_plan[i]['all_red']
                        if phase_plan[i]['stage_flow'] == 0.0:
                            phase_plan[i]['stage_flow'] = min_stage_time / phase_plan[i]['sat_headway_time']

                        phase_plan[i]['stage_time'] = max(min_stage_time, phase_plan[i]['stage_flow'] * phase_plan[i][
                            'sat_headway_time'])
        return phase_plan

    ###如果阶段的时长小于最小绿要求，则将最小绿作为阶段配时，并计算清空系数（最小绿/阶段时长）；
    def iterate_clear_ratio(self, phase_plan, overall_ratio):
        for i in range(0, len(phase_plan)):
            min_stage_time = max(phase_plan[i]['min_green'], phase_plan[i]['pedestrian_time']) + phase_plan[i][
                'yellow'] + \
                             phase_plan[i]['all_red']
            if phase_plan[i]['stage_time'] * overall_ratio < min_stage_time:
                phase_plan[i]['stage_clear_ratio'] = (phase_plan[i]['stage_clear_ratio'] / overall_ratio) * (
                        min_stage_time / phase_plan[i]['stage_time'])

            else:
                phase_plan[i]['stage_clear_ratio'] *= overall_ratio

    def calculate_cycle_time(self,phase_plan):
        iterative_cycle = 0.0
        for i in range(0, len(phase_plan)):
            phase_plan[i]['stage_time'] = phase_plan[i]['stage_flow'] * phase_plan[i]['sat_headway_time'] * \
                                          phase_plan[i][
                                              'stage_clear_ratio']
            phase_plan[i]['stage_time'] += (self.start_loss + phase_plan[i]['all_red'])
            iterative_cycle += phase_plan[i]['stage_time']
        return iterative_cycle

    ###如果阶段总体的周期时长小于传入的时长，则将计算所有清空系数（传入时长/周期时长）；
    ###如果阶段总体的周期时长大于传入的时长，则在保证最小绿情况下，计算其他阶段的清空系数（传入时长-阶段达到最小绿）/（周期时长-阶段达到最小绿）
    ###将阶段流量乘以清空系数，得到换算后的流量，重新分配绿灯；
    ###直到接近传入的时长。
    def allocate_green_time(self,phase_plan, cycle):
        overall_ratio = 1.0
        self.iterate_clear_ratio(phase_plan, overall_ratio)
        iterative_cycle = self.calculate_cycle_time(phase_plan)

        while abs(iterative_cycle - cycle) >= 0.5:
            overall_ratio = cycle / iterative_cycle
            self.iterate_clear_ratio(phase_plan, overall_ratio)
            iterative_cycle = self.calculate_cycle_time(phase_plan)
            # print(iterative_cycle)
        return phase_plan, iterative_cycle

    def stage_to_ring(self, phase_plan):
        ring_num = 2
        ring_list = [[] for i in range(0, ring_num)]

        ring_col_index = {}
        for i in range(0, ring_num):
            ring_col_index[i] = 0

        overlap_phases = {}
        overlap = {}

        for i in range(0, len(phase_plan)):
            stage_info = phase_plan[i]['id']
            stage_time = phase_plan[i]['stage_time']
            yellow = phase_plan[i]['yellow']
            all_red = phase_plan[i]['all_red']
            # 提取阶段里的所有相位
            ring_phases_no = [[] for i in range(0, ring_num)]
            for j in range(0, len(stage_info)):
                if stage_info[j] in ('1', '2', '3', '4'):
                    ring_phases_no[0].append(stage_info[j])
                elif stage_info[j] in ('5', '6', '7', '8'):
                    ring_phases_no[1].append(stage_info[j])
                else:
                    overlap_phases[stage_info[j]] = []
            for j in range(0, len(stage_info)):
                if stage_info[j] not in ('1', '2', '3', '4', '5', '6', '7', '8'):
                    # 存储搭接相位的相位值集合
                    for m in range(0, ring_num):
                        overlap_phases[stage_info[j]].extend(ring_phases_no[m])

            for m in range(0, ring_num):
                if len(ring_phases_no[m]) == 0:
                    continue

                # 存储主相位的编号
                ring_main_phase = 0
                if len(ring_phases_no[m]) == 1:
                    ring_main_phase = ring_phases_no[m][0]
                else:
                    # 按照直行、左转的顺序确定主相位
                    straight_list = [no for no in ring_phases_no[m] if int(no) % 2 == 0]
                    left_list = [no for no in ring_phases_no[m] if int(no) % 2 == 1]
                    if len(straight_list) > 0:
                        ring_main_phase = straight_list[0]
                    elif len(left_list) > 0:
                        ring_main_phase = left_list[0]
                    else:
                        ring_main_phase = ring_phases_no[m][0]

                    ring_phases_no[m].remove(ring_main_phase)
                    for overlap_p in ring_phases_no[m]:
                        if overlap_p in overlap_phases.keys():
                            overlap_phases[overlap_p].append(ring_main_phase)
                        else:
                            overlap_phases[overlap_p] = [ring_main_phase]

                # 如果当前阶段的各环主相位编号与数组对应行的前一个元素的主相位编号不相同时，则将新建的字典添加到对应行数组中
                if len(ring_list[m]) == 0:
                    dict_info = {'id': ring_main_phase, 'start': 0.0, 'end': stage_time, 'overlap': '', 'barrier': '0',
                                 'yellow': yellow, 'all_red': all_red}
                    ring_list[m].append(dict_info)
                elif ring_main_phase != ring_list[m][ring_col_index[m]]['id']:
                    # ring_list[m][ring_col_index[m]]['end'] += (
                    #             ring_list[m][ring_col_index[m]]['yellow'] + ring_list[m][ring_col_index[m]]['all_red'])
                    dict_info = {'id': ring_main_phase, 'start': ring_list[m][ring_col_index[m]]['end'], 'end': 0.0,
                                 'overlap': '', 'barrier': '0', 'yellow': yellow, 'all_red': all_red}
                    dict_info['end'] = dict_info['start'] + stage_time
                    ring_list[m].append(dict_info)
                    ring_col_index[m] += 1
                else:
                    # 如果当前阶段的各环主相位编号与数组对应行的前一个元素的主相位编号相同，则直接更新前一个元素的结束时间 += 阶段时长
                    ring_list[m][ring_col_index[m]]['end'] = ring_list[m][ring_col_index[m]][
                                                                 'end'] + stage_time  # + yellow
                    ring_list[m][ring_col_index[m]]['yellow'] = yellow
                    ring_list[m][ring_col_index[m]]['all_red'] = all_red

            # 更新相位后的屏障
            flag = False
            barrier_condition = [[('1', '2'), ('3', '4')],
                                 [('5', '6'), ('7', '8')]]

            for m in range(0, ring_num):
                if (len(ring_list[m]) == 0) or (len(ring_list[m]) == 1):
                    continue
                length = len(barrier_condition[m])
                for b in range(0, length):
                    if (ring_list[m][ring_col_index[m] - 1]['id'] in barrier_condition[m][b]) & (
                            ring_list[m][ring_col_index[m]]['id'] in barrier_condition[m][length - 1 - b]):
                        flag = True
            # 当数组第一行的相位编号由1（或者2）互换为3（或者4），或者数组第二行的相位编号由5（或者6）互换为7（或者8）时，同时更新第一行和第二行的前一个元素的相位后屏障值；
            if flag:
                for m in range(0, ring_num):

                    if len(ring_list[m]) == 1:
                        dict_info = {'id': '0', 'start': 0.0, 'end': 0.0, 'overlap': '', 'barrier': '1', 'yellow': 0,
                                     'all_red': 0}
                        ring_list[m].insert(0, dict_info)
                    else:
                        ring_list[m][ring_col_index[m] - 1]['barrier'] = '1'

            # 或者遍历结束时，同时更新第一行和第二行的前一个元素的相位后屏障值；
            if i == len(phase_plan) - 1:
                for m in range(0, ring_num):
                    ring_list[m][ring_col_index[m]]['barrier'] = '1'
                    # ring_list[m][ring_col_index[m]]['end'] += ring_list[m][ring_col_index[m]]['yellow'] + \
                    #                                           ring_list[m][ring_col_index[m]]['all_red']

        # 添加搭接相位
        for key, value in overlap_phases.items():
            if len(value) == 1:
                value = value[0]
                if value not in overlap.keys():
                    overlap[value] = [key]
                else:
                    overlap[value].append(key)
            else:
                # 如果值集合中有元素出现多次，将该元素作为主相位
                value_count = Counter(value)
                max_value = 1
                main_phase = ''
                for k, value in value_count.items():
                    if value > max_value:
                        main_phase = k
                        max_value = value
                if max_value > 1:
                    if main_phase not in overlap.keys():
                        overlap[main_phase] = [key]
                    else:
                        overlap[main_phase].append(key)
                    continue

                # 如果值集合中元素只出现一次，并且集合元素数大于1，当存在同方向的直行/左转时，按优先级先在直行相位上，添加该搭接相位
                if 'P' in key:
                    straight_phase = key.split('P')[1]
                    if straight_phase in value_count.keys():
                        if straight_phase not in overlap.keys():
                            overlap[straight_phase] = [key]
                        else:
                            overlap[straight_phase].append(key)

        for m in range(0, ring_num):
            for n in range(0, len(ring_list[m])):

                # 环相位添加搭接相位
                if ring_list[m][n]['id'] in overlap.keys():
                    ring_list[m][n]['overlap'] = self.list_to_str(overlap[ring_list[m][n]['id']])

                # 环相位添加通行能力值
                for _, values in self.phase_lane.items():
                    for _, value in values.items():
                        if value['phase'] == ring_list[m][n]['id']:
                            ring_list[m][n]['sat_flow'] = float(value['sat_flow'])
        return ring_list

    def initial_queue_calculation(self, phase_plan, flow, iterative_cycle):
        ring_phases_info = self.stage_to_ring(phase_plan)
        ring_phases_queue = ring_phases_info
        for i in range(0, self.ring_num):
            for j in range(0, len(ring_phases_queue[i])):
                ring_phases_queue[i][j]['initial_queue_length'] = 0.0
                ring_phases_queue[i][j]['max_queue_length'] = 0.0
                ring_phases_queue[i][j]['min_queue_length'] = 0.0

        ##计算排队长度
        ###以进口道为统计单位，统计进口道的平均车道流量，作为上游的流量；以绿灯期间的车道通行能力作为下游的放行流量，而红灯期间下游放行流量为0；
        def calculate_capacity_under_control(sr):
            base_capacity, phase_capacity = 1200.0, 1200.0
            for i in range(0, self.ring_num):
                for j in range(0, len(ring_phases_info[i])):
                    if sr['phase'] in (ring_phases_info[i][j]['id'] + ',' + ring_phases_info[i][j]['overlap']):
                        base_capacity = ring_phases_info[i][j]['sat_flow']
                        phase_capacity = ring_phases_info[i][j]['sat_flow'] * (
                                ring_phases_info[i][j]['end'] - ring_phases_info[i][j]['start'] +
                                ring_phases_info[i][j]['yellow'] - self.start_loss) / iterative_cycle
                        if 'phase_capacity' in sr.index.tolist():
                            phase_capacity += sr['phase_capacity']
            return pd.Series([base_capacity, phase_capacity])

        ###上游的速度，按照经验公式，通过流量计算出速度（如果进口道的平均车道流量>进口道通行能力的0.8，表示饱和状态；否则为不饱和状态）；
        def calculate_speed_from_flow(sr):
            # 饱和状态区间的最大速度设为35km/h；不饱和状态区间的最大速度设为60km/h,最小速度设为40km/h(交通流手册，48页),车道默认最大的通行能力为1800pcu/h
            max_speed_sat = 35
            max_speed_unsat = 60
            min_speed_unsat = 40
            # 标定流量与速度曲线的系数：(城市干道路段速度_流量模型及通行能力研究,https://max.book118.com/html/2017/0423/101864563.shtm)
            ##饱和状态下：
            b0 = 6.9671
            b1 = math.log(max_speed_sat / b0) / sr['road_ave_capacity']
            ##不饱和状态下：
            a0 = 60
            a1 = (min_speed_unsat - max_speed_unsat) / sr['road_ave_capacity']
            # 按通行能力换算当前流量
            # volume = sr['road_sum_capacity'] / sr['down_capacity'] * sr['road_flow_rate']  # TODO:如何改变上游进口道的总换算流量,sr['road_sum_capacity']/sr['down_capacity']应该是个固定值，现在随下游通行能力改变
            volume = sr['road_flow_rate']
            # 根据流量计算速度
            # sr['sat_ratio'] = 0.7
            if sr['sat_ratio'] >= self.sat_ratio_threshold:
                speed = b0 * math.exp(b1 * volume)
            else:
                speed = a0 + a1 * volume
            return speed

        if 'phase_capacity' in flow.columns.tolist():
            flow = flow.drop(columns=['phase_capacity'])

        flow[['base_capacity', 'phase_capacity']] = flow.apply(calculate_capacity_under_control, axis=1)
        flow['road_sum_capacity'] = flow.groupby(['day_no', 'period_no', 'plan_no', 'time', 'road'])[
            'base_capacity'].transform('sum')
        flow['road_ave_capacity'] = flow.groupby(['day_no', 'period_no', 'plan_no', 'time', 'road'])[
            'base_capacity'].transform('mean')
        flow['down_flows'] = flow.groupby(['day_no', 'period_no', 'plan_no', 'time', 'road'])[
            'flow_rate'].transform('sum')
        flow['down_capacity'] = flow.groupby(['day_no', 'period_no', 'plan_no', 'time', 'road'])[
            'phase_capacity'].transform('sum')
        flow['sat_ratio'] = flow['down_flows'] / flow['down_capacity']  # 暂时仅用于评价交通状态（分为饱和状态和非饱和状态）
        flow['up_speed'] = flow.apply(calculate_speed_from_flow, axis=1)
        return flow, ring_phases_queue

    ##将阶段转换为环结构，遍历环的相位时长，计算每个相位的排队长度：
    ###按照红灯阶段的排队形成波和绿灯阶段的排队消散波，分别计算形成波速wf和消散波速wd1；并按照几何规则，计算两波相遇的位置和周期内的相对时间点，分别作为最大排队长度值和排队最大时刻；
    ####如果最大排队长度超过了路段的长度，缩短红灯的持续时间（传入周期时长-当前相位的绿灯时长？？？）
    ###如果排队最大时刻小于相位的绿灯结束时刻时，形成第二个消散波wd2，计算在绿灯结束时刻的排队长度，作为最小排队长度值。
    ###取相位的最大排队长度和最小排队长度的均值，作为相位的排队长度，进而将所有相位的排队长度的均值，作为交叉口的排队长度值。

    def calculate_junction_queue_length(self, flow, ring_phases_queue, iterative_cycle):
        junction_queue_length = 0.0

        for i in range(0, self.ring_num):
            for j in range(0, len(ring_phases_queue[i])):

                def caculate_max_flow_phase(overlap, flow, phase_id):
                    flow_info = flow[flow['phase'] == phase_id]
                    max_flow = 0.0
                    if flow_info.shape[0] > 0:
                        max_flow = flow[flow['phase'] == phase_id].iloc[0]['flow_rate']

                    if overlap != '':
                        ring_list = overlap.split(',')
                        for ring_p in ring_list:
                            flow_info = flow[flow['phase'] == ring_p]
                            if flow_info.shape[0] == 0:
                                continue
                            if flow_info.iloc[0]['flow_rate'] > max_flow:
                                phase_id = ring_p
                                max_flow = max(max_flow, flow_info.iloc[0]['flow_rate'])
                    return phase_id

                phase_id = caculate_max_flow_phase(ring_phases_queue[i][j]['overlap'], flow,
                                                   ring_phases_queue[i][j]['id'])
                phase_flow_info = flow[flow['phase'] == phase_id]
                if len(phase_flow_info) == 0:
                    continue
                # print('phase_id: ', ring_phases_queue[i][j]['id'])
                # 红灯阶段的排队形成波
                # phase_flow_info.iloc[0]['road_flow_rate']        #按通行能力换算当前流量
                # phase_flow_info.iloc[0]['road_sum_capacity']/phase_flow_info.iloc[0]['down_capacity'] * phase_flow_info.iloc[0]['road_flow_rate']
                up_volume = phase_flow_info.iloc[0]['flow_rate']  # TODO：如何设计当前相位的总流量。需要知道相位在上游路段时，对应的流量
                speed_wf1 = (0 - up_volume) / (self.stop_density - up_volume / phase_flow_info.iloc[0]['up_speed'])

                # 绿灯阶段的第一个排队消散波
                sat_pass_headway = 30 / 3.6 * 3600 / ring_phases_queue[i][j][
                    'sat_flow']  # 绿灯饱和通行状态下的密度，放行速度不低于路段速度的50%-70%（直行最大）（城市道路设计规范）
                sat_pass_density = 1000 / sat_pass_headway
                speed_wd1 = (phase_flow_info.iloc[0]['base_capacity'] - 0) / (sat_pass_density - self.stop_density)

                # print('speed_wf1:',speed_wf1/3.6)
                # print('speed_wd1:',speed_wd1/3.6)

                ##当前相位下，寻找前一红灯持续的时长
                index_list = list(range(j - 1, -1, -1)) + list(range(len(ring_phases_queue[i]) - 1, j, -1))

                red_time_before_phase = iterative_cycle - (
                        ring_phases_queue[i][j]['end'] - ring_phases_queue[i][j]['start'])
                for r in index_list:
                    if ring_phases_queue[i][r]['id'] == ring_phases_queue[i][j]['id']:
                        if ring_phases_queue[i][j]['start'] < ring_phases_queue[i][r]['end']:
                            red_time_before_phase = iterative_cycle - ring_phases_queue[i][r]['end'] + \
                                                    ring_phases_queue[i][j]['start']
                        else:
                            red_time_before_phase = ring_phases_queue[i][j]['start'] - ring_phases_queue[i][r]['end']
                        break

                # 绿灯期间，两个波相遇的位置和周期内的相对时间点，分别作为最大排队长度值和排队最大时刻
                phase_max_queue_time = (ring_phases_queue[i][j]['initial_queue_length'] + abs(
                    speed_wf1) / 3.6 * red_time_before_phase) / (abs(speed_wd1 - speed_wf1) / 3.6)
                phase_green_time = ring_phases_queue[i][j]['end'] - ring_phases_queue[i][j]['start'] - \
                                   ring_phases_queue[i][j]['all_red']

                if phase_max_queue_time > phase_green_time:
                    ring_phases_queue[i][j]['max_queue_length'] = iterative_cycle * abs(speed_wf1) / 3.6
                    ring_phases_queue[i][j]['min_queue_length'] = ring_phases_queue[i][j]['max_queue_length']
                    ring_phases_queue[i][j]['initial_queue_length'] = ring_phases_queue[i][j]['max_queue_length']

                    junction_queue_length += (ring_phases_queue[i][j]['min_queue_length'] + ring_phases_queue[i][j][
                        'max_queue_length']) / 2

                    # print('max_queue_length:', ring_phases_queue[i][j]['max_queue_length'])
                    # print('min_queue_length:', ring_phases_queue[i][j]['min_queue_length'])
                    continue
                else:
                    ring_phases_queue[i][j]['max_queue_length'] = phase_max_queue_time * abs(speed_wd1) / 3.6

                # print('max_queue_time:%f ,green_time:%f ' % (phase_max_queue_time, phase_green_time))

                # 如果排队最大时刻小于相位的绿灯结束时刻时，形成第二个消散波wd2，计算在绿灯结束时刻的排队长度，作为最小排队长度值。
                ##绿灯阶段的第二个排队消散波

                speed_wd2 = (phase_flow_info.iloc[0]['base_capacity'] - up_volume) / (
                        sat_pass_density - up_volume / phase_flow_info.iloc[0]['up_speed'])
                # print('speed_wd2:',speed_wd2/3.6)
                max_queue_dissipation_time = ring_phases_queue[i][j]['max_queue_length'] / abs(speed_wd2 / 3.6)

                if (phase_max_queue_time + max_queue_dissipation_time) < phase_green_time:
                    ring_phases_queue[i][j]['min_queue_length'] = 0.0
                else:
                    speed_wf2 = (0 - phase_flow_info.iloc[0]['base_capacity']) / (self.stop_density - sat_pass_density)
                    # print('speed_wf2:',speed_wf2)
                    phase_min_queue_time = (ring_phases_queue[i][j]['max_queue_length'] - abs(speed_wd2 / 3.6) * (
                            phase_green_time - phase_max_queue_time)) / abs((speed_wd2 - speed_wf2) / 3.6)
                    # print('min_queue_time:', phase_min_queue_time)
                    ring_phases_queue[i][j]['min_queue_length'] = phase_min_queue_time * abs(speed_wf2) / 3.6
                    # print('min_queue_length:', ring_phases_queue[i][j]['min_queue_length'])
                    ring_phases_queue[i][j]['initial_queue_length'] = ring_phases_queue[i][j]['min_queue_length']

                # print('max_queue_length:', ring_phases_queue[i][j]['max_queue_length'])
                # print('min_queue_length:', ring_phases_queue[i][j]['min_queue_length'])

                ###取相位的最大排队长度和最小排队长度的均值，作为相位的排队长度，进而将所有相位的排队长度的均值，作为交叉口的排队长度值。
                junction_queue_length += (ring_phases_queue[i][j]['min_queue_length'] + ring_phases_queue[i][j][
                    'max_queue_length']) / 2

        return junction_queue_length

    # 具体步骤：
    ##由松弛条件下的初始可行解，并得到交叉口的排队长度值。建立搜索树结构，根节点表示所有可行解（以哨兵方式建立），并标记根节点的层为0；上界设为0，建树过程中只更新上界值；
    ##第一层分支，计算阶段一的时长范围，最小值为最小绿，最大值为周期时长减去剩余阶段的最小绿之和；
    ##在区间中按照步长，并标记所在层；
    ###遍历当前层的拓展节点时，相当于已知阶段一的整数绿灯时长时，计算截至当前阶段的排队长度和，并估计剩余阶段的排队长度和，从而估计交叉口排队长度值；按照交叉口的排队长度值，将当前阶段时长的对应拓展节点存储在优先队列中（优先队列的元素中，保存交叉口累计排队长度的估计值、当前阶段时长、当前层、父节点）；并将父节点从优先队列中剔除；
    ###遍历该优先队列，进入第二层分支，类似过程与第一层分支类似。
    ###如果当前阶段的节点是叶节点（即当前阶junction_queue_pred段到达最大阶段数），将该节点计算的交叉口排队长度作为上界；
    ###继续遍历优先队列里的节点，如果节点对应的排队长度值大于该上界时（即使没达到叶节点），则直接返回（因而拓展节点采用优先队列存储，从而实现剪枝的目的），表示完成算法过程；
    ###当优先队列为空时，也表示完成算法过程。
    def branch_and_bound(self, phase_plan, stages_priority_queue, flow, iterative_cycle, initial_cycle):

        result = []

        junction_queue_threshold = sys.float_info.max

        def generate_stage_time(min_green, max_green):
            stage_time_list = list(range(min_green, max_green, self.step))
            if max_green not in stage_time_list:
                stage_time_list.append(max_green)
            return stage_time_list

        stage_no, stage_num = 0, len(phase_plan)

        while 1:

            if stages_priority_queue.empty():
                #print(result)
                break

            junction_queue, stage_node = stages_priority_queue.get()
            #         print('"""""'*5)
            #         print('junction_queue: %f'%junction_queue)
            #         print('cumulative_stage_time: %d'%stage_node['cumulative_stage_time'])

            if junction_queue >= junction_queue_threshold:  # 剪枝
                #print(result)
                break

            stage_no = stage_node['layer']

            phase_plan = stage_node['phase_plan']
            phase_plan_rest = phase_plan[stage_no + 1:stage_num]

            min_green = phase_plan[stage_no]['min_green'] + phase_plan[stage_no]['yellow'] + phase_plan[stage_no][
                'all_red']  # 可考虑排队空间
            max_green = math.ceil(phase_plan[stage_no]['stage_time'])  # 预估的某一可行阶段时长

            cumulative_min_green = 0.0
            for s in range(stage_no + 1, stage_num):
                cumulative_min_green += phase_plan[s]['min_green'] + phase_plan[s]['yellow'] + phase_plan[s]['all_red']
            max_green_2 = math.floor(iterative_cycle - stage_node[
                'cumulative_stage_time'] - cumulative_min_green)  # 加入最大绿灯时间的限制，通过后续的阶段的最大绿灯计算
            # print('iterative_cycle: %f,cumulative_stage_time: %f,cumulative_min_green: %f,max_green_2:%d'%(iterative_cycle,stage_node['cumulative_stage_time'],cumulative_min_green,max_green_2))

            stage_time_list = generate_stage_time(min_green, max_green_2)  # 根据最小和最大绿灯时长，产生当前阶段的可行绿灯时长列表

            for i in range(0, len(stage_time_list)):  # 分支

                # 初始化相位的清空系数为1.0
                for j in range(0, len(phase_plan_rest)):
                    phase_plan_rest[j]['stage_clear_ratio'] = 1.0

                # 将当前阶段的时长置为遍历值
                phase_plan[stage_no]['stage_time'] = stage_time_list[i]
                # print("stage_no:%d, stage_time:%d"%(stage_no,stage_time_list[i]))
                # 创建节点
                current_stage_node = {'junction_queue_pred': 0.0, 'current_stage_time': stage_time_list[i],
                                      'cumulative_stage_time': stage_node['cumulative_stage_time'] + stage_time_list[i],
                                      'layer': stage_no + 1,
                                      'phase_plan': []}

                cycle_rest = iterative_cycle - current_stage_node['cumulative_stage_time']

                # 计算剩余阶段的绿灯分配时长，得到phase_plan_rest(包含新的stage_time)和剩余的阶段时长和
                phase_plan_rest, cycle_rest = self.allocate_green_time(phase_plan_rest, cycle_rest)
                # 拼接完整的阶段方案phase_plan
                phase_plan = phase_plan[0:stage_no + 1] + phase_plan_rest
                # 估计交叉口的排队总长
                flow, ring_phases_queue = self.initial_queue_calculation(phase_plan, flow, iterative_cycle)
                junction_queue_pred = self.calculate_junction_queue_length(flow, ring_phases_queue, iterative_cycle)
                # print("junction_queue_pred: %f"%junction_queue_pred)
                # print()
                current_stage_node['junction_queue_pred'] = junction_queue_pred
                new_phase_plan = copy.deepcopy(phase_plan)
                current_stage_node['phase_plan'] = new_phase_plan

                if junction_queue_pred < junction_queue_threshold:

                    # 按照交叉口的排队长度值，将当前阶段时长的对应拓展节点存储在优先队列中
                    stages_priority_queue.put((junction_queue_pred, current_stage_node))

                    if stage_no + 1 == stage_num - 1:
                        phase_plan[stage_no + 1]['stage_time'] = int(
                            initial_cycle - current_stage_node['cumulative_stage_time'])
                        # 更新排队长度的上界值junction_queue_threshold
                        junction_queue_threshold = junction_queue_pred
                        result = phase_plan
        junction_queue_threshold *= self.flow_interval * 60 / initial_cycle
        # print('junction_queue: ', junction_queue_threshold)
        # print()
        return junction_queue_threshold, result

    def muti_object_optimize(self):
        # 生成每个时刻点的配时方案
        key_phase_flow = self.get_key_phase_flow_rate()
        times_info = key_phase_flow.groupby(['day_no', 'period_no', 'plan_no', 'time']).agg(
            {'phase': 'count'}).reset_index()

        for i in range(0,times_info.shape[0]):
            time_point = times_info.iloc[i]['time']
            flow = self.algorithm_input(key_phase_flow, time_point)

            result = []
            min_junction_queue = sys.float_info.max

            cycle_list = []
            cycles_queue_map = {}
            cycles_priority_queue = PriorityQueue()

            phase_plan = copy.deepcopy(self.plan_para['phase_plan'])
            ring_overlap = self.judge_main_phase_overlap()

            min_cycle = 0
            for i in range(0, len(self.plan_para['phase_plan'])):
                min_green = self.plan_para['phase_plan'][i]['min_green']
                yellow = self.plan_para['phase_plan'][i]['yellow']
                all_red = self.plan_para['phase_plan'][i]['all_red']
                min_cycle += (min_green + yellow + all_red)
            min_cycle = max(min_cycle, self.plan_para['min_cycle'])
            max_cycle = self.plan_para['max_cycle']

            # 不满足周期范围时，取区间的中间值作为周期时长
            if max_cycle - min_cycle <= 2 * self.step:
                cycle = min_cycle + math.floor((max_cycle - min_cycle) / (2 * self.step)) * self.step

                # 调用周期内的计算函数，计算出junction_queue_pred
                flow['cycle_flow'] = flow['flow_rate'] / 3600 * cycle
                flow_input = self.return_flow_input(ring_overlap, flow)  # 返回字典，每个相位的流量

                phase_plan = self.caculate_stage_flow(flow_input, ring_overlap, phase_plan)

                # 第一次生成方案，初始化优先队列
                phase_plan, iterative_cycle = self.allocate_green_time(phase_plan, cycle)
                flow, ring_phases_queue = self.initial_queue_calculation(phase_plan, flow, iterative_cycle)
                junction_queue_pred = self.calculate_junction_queue_length(flow, ring_phases_queue, iterative_cycle)

                stages_priority_queue = PriorityQueue()

                stage_node = {'junction_queue_pred': junction_queue_pred,
                              'current_stage_time': 0.0,
                              'cumulative_stage_time': 0.0,
                              'layer': 0,
                              'phase_plan': phase_plan}
                stages_priority_queue.put((junction_queue_pred, stage_node))

                # 分支定界法搜索方法
                junction_queue_threshold, phase_plan = self.branch_and_bound(phase_plan, stages_priority_queue, flow,
                                                                        iterative_cycle, cycle)
                result = copy.deepcopy(phase_plan)

            while max_cycle - min_cycle > 2 * self.step:  # 周期搜索步长的体现地方

                middle_cycle = min_cycle + math.floor((max_cycle - min_cycle) / (2 * self.step)) * self.step
                cycle_first = middle_cycle
                cycle_second = middle_cycle + self.step
                cycle_list.append(cycle_first)
                cycle_list.append(cycle_second)

                # print('min_cycle: %d, max_cycle:%d' % (min_cycle, max_cycle))
                # print('cycle_list', cycle_list)

                for c in cycle_list:

                    if c in cycles_queue_map.keys():
                        junction_queue_threshold = cycles_queue_map[c]['junction_queue']
                        phase_plan = cycles_queue_map[c]['phase_plan']
                    else:

                        # 调用周期内的计算函数，计算出junction_queue_pred
                        flow['cycle_flow'] = flow['flow_rate'] / 3600 * c
                        flow_input = self.return_flow_input(ring_overlap, flow)  # 返回字典，每个相位的流量

                        phase_plan = self.caculate_stage_flow(flow_input, ring_overlap, phase_plan)

                        # 第一次生成方案，初始化优先队列
                        phase_plan, iterative_cycle = self.allocate_green_time(phase_plan, c)
                        flow, ring_phases_queue = self.initial_queue_calculation(phase_plan, flow, iterative_cycle)
                        junction_queue_pred = self.calculate_junction_queue_length(flow, ring_phases_queue, iterative_cycle)

                        stages_priority_queue = PriorityQueue()

                        stage_node = {'junction_queue_pred': junction_queue_pred,
                                      'current_stage_time': 0.0,
                                      'cumulative_stage_time': 0.0,
                                      'layer': 0,
                                      'phase_plan': phase_plan}
                        stages_priority_queue.put((junction_queue_pred, stage_node))

                        # 分支定界法搜索方法
                        junction_queue_threshold, phase_plan = self.branch_and_bound(phase_plan, stages_priority_queue, flow,
                                                                                iterative_cycle, c)

                        # 加入到周期长度的缓存字典中
                        queue_info = {'junction_queue': junction_queue_threshold, 'phase_plan': phase_plan}
                        cycles_queue_map[c] = queue_info

                    if junction_queue_threshold < min_junction_queue:
                        result = copy.deepcopy(phase_plan)
                        min_junction_queue = junction_queue_threshold

                    # cycle_node = {'junction_queue_pred': junction_queue_threshold,
                    #               'cycle': c,
                    #               'phase_plan': phase_plan}
                    # cycles_priority_queue.put((junction_queue_threshold, cycle_node))
                if cycles_queue_map[cycle_list[0]]['junction_queue'] < cycles_queue_map[cycle_list[1]]['junction_queue']:
                    max_cycle = cycle_list[1]
                else:
                    min_cycle = cycle_list[0]

                cycle_list.clear()

            # 将结果添加到配时参数结果中：
            for r in range(0, len(result)):
                stage_no = 'P' + str(r + 1)
                self.timing = self.timing.append({'day_no': times_info.iloc[i]['day_no'],
                                        'period_no': times_info.iloc[i]['period_no'],
                                        'plan_no': times_info.iloc[i]['plan_no'],
                                        'time': time_point,
                                        'stage_no': stage_no,
                                        'phase_time': result[r]['stage_time'],
                                        'yellow': result[r]['yellow'],
                                        'all_red': result[r]['all_red'],
                                        'yi': result[r]['stage_flow']}, ignore_index=True)

    def timing_cluster(self):
        """
        获取聚类标签
        """
        self.timing = self.timing.reset_index(drop=True)
        # 增加车流变化率特征
        for _, group in self.timing.groupby(['day_no', 'period_no', 'plan_no', 'stage_no']):
            group.sort_values(by='time', inplace=True)
            self.timing.loc[group.index, 'slope'] = group['yi'].diff() / group['yi'].shift(1)

        self.timing.fillna(method='bfill', inplace=True)

        # 对峰值时段内的时隙进行聚类
        self.timing['cluster_label'] = -1  # 平峰聚类标签统一设置为-1
        for name, group in self.timing.groupby(['day_no', 'period_no', 'plan_no']):
            # if 'flat' in name[-1]:
            # continue
            X = group.pivot(index='time', columns='stage_no', values=['slope', 'phase_time']).sort_index()
            X['n'] = np.arange(len(X)) * 6
            X.replace([np.inf, -np.inf], 0, inplace=True)
            X.fillna(0, inplace=True)
            time_label_dict = self._cluster(X)
            self.timing.loc[group.index, 'cluster_label'] = group.time.map(time_label_dict)

        self.timing = self.timing.astype({'cluster_label': int}).drop(columns='slope')

    def _cluster(self,X):
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

        self.timing['green'] = self.timing.groupby(['stage_no', 'day_no', 'period_no', 'plan_no', 'cluster_label'])[
            'phase_time'].transform(green_mean_quantile).astype(int)  # 绿灯时长直接取整，.apply(lambda x:math.ceil(x))
        self.timing = self.timing.reset_index(drop=True)

    def get_result(self):
        self.timing = self.timing[['day_no', 'period_no', 'plan_no', 'time', 'stage_no', 'green', 'yellow', 'all_red']]

        subcolumns = self.timing.columns.tolist()
        subcolumns.remove('time')
        self.time_out = self.timing.sort_values(['day_no', 'period_no', 'time']).drop_duplicates(
            subcolumns).reset_index(drop=True)
        self.time_out['phase_time'] = self.time_out['green']
        self.time_out['cycle'] = self.time_out.groupby(['day_no', 'period_no', 'plan_no'])['phase_time'].transform('sum')

    def auto_timing(self):
        print('2、开始进行配时分析……')
        self.muti_object_optimize()
        print('- 多目标配时完成')
        self.timing_cluster()
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
