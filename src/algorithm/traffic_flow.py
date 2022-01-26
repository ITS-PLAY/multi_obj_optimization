import sys
import pandas as pd
import numpy as np
import time
import xml.dom.minidom as XML

class Traffic_Flow:

    def __init__(self, data_file, traffic_light_file, inter_id, plan_para={}, flow_interval=6):
        self.data_file = data_file
        self.traffic_light_file = traffic_light_file

        self.plan_para = plan_para
        self.stage_phases = {}  # {key:value} == {phase_no:stage_no}

        self.inter_id = inter_id
        self.flow_interval = flow_interval
        # 渠化映射表
        self.detect_road = {}  # {key:value} == {detect:road}
        self.phase_lane = {}  # {key:{key:{key:value}}} == {camera:{lane:{phase:value,sat_flow:value}}}
        # 时段映射表
        self.schedule = {}     # {key:{key:value}} == {schedule_no:{node_name:node_value}}
        self.day = {}          # {key:{key:{key:value}} == {day_no:{start_time:{node_name:node_value}}
        # 方案映射表
        self.links_no = ''
        # self.plan = {}                         #{key:value} == {plan_no:links_no}

        self.plan_phases = {}  # {key:value} == {plan_no:xml.nodelist}

        self.flows = None

        domTree = XML.parse(self.traffic_light_file)
        self.rootNode = domTree.documentElement

    def read_XML(self):
        self.read_schedule_info()
        self.read_day_info()
        # self.read_plan_info()     #注释方案与渠化编号的对应，默认只有一个渠化编号，20220107

    # 将列表拼接成字符串
    def list_to_str(self, detect_ids):
        id_string = ''
        for j in range(0, len(detect_ids)):
            id_string += "%s" % detect_ids[j] + ','
        id_string = id_string[:-1]
        return id_string

    # 读取路段中的进口道-检测器映射信息，以及进口道-车道-相位的映射信息
    def read_links(self):
        detector_ids = ""
        lights = self.rootNode.getElementsByTagName("light")  # 存在多个交叉口
        for light in lights:
            if light.hasAttribute("id") and light.getAttribute("id") == self.inter_id:
                print("junction_id:", light.getAttribute("id"))
                all_links = light.getElementsByTagName("links")  # 存在多个links
                for all_link in all_links:
                    if all_link.hasAttribute("no"):
                        self.links_no = all_link.getAttribute("no")
                        links = all_link.getElementsByTagName("link")
                        for link in links:
                            road = link.getAttribute("from")
                            # 读取XML文件，返回检测器与路段的映射表，和转换成的字符串
                            if link.hasAttribute("camera"):
                                camera = int(link.getAttribute("camera"))  # camear_id为整型
                                self.detect_road[camera] = road

        detect_ids = list(self.detect_road.keys())
        detector_ids = self.list_to_str(detect_ids)  # 交叉口多个检测器的拼接字符串，以逗号隔开
        return detector_ids

    # 读取过车数，返回过车数据的起始和结束时间，和流量数据
    def read_data(self):
        camera_ids = self.read_links()
        vehicle_flow = pd.read_csv(self.data_file)
        vehicle_flow = vehicle_flow.query('camera_id in (%s)' % camera_ids)
        vehicle_flow['passtime'] = pd.to_datetime(vehicle_flow['passtime'])
        return vehicle_flow

    def read_schedule_info(self):  # 读取调度信息
        sches = self.rootNode.getElementsByTagName("schedule")
        for sche in sches:
            if sche.hasAttribute("no"):
                no = sche.getAttribute("no")

                schedule_info = {}
                start_month = (sche.getElementsByTagName("start_month")[0]).childNodes[0].data
                start_day = (sche.getElementsByTagName("start_day")[0]).childNodes[0].data
                end_month = (sche.getElementsByTagName("end_month")[0]).childNodes[0].data
                end_day = (sche.getElementsByTagName("end_day")[0]).childNodes[0].data
                week = (sche.getElementsByTagName("week")[0]).childNodes[0].data
                day_no = (sche.getElementsByTagName("day_no")[0]).childNodes[0].data
                schedule_info['start_month'] = start_month
                schedule_info['start_day'] = start_day
                schedule_info['end_month'] = end_month
                schedule_info['end_day'] = end_day
                schedule_info['week'] = week
                schedule_info['day_no'] = day_no
                self.schedule[no] = schedule_info

    def read_day_info(self):  # 读取日计划信息
        days = self.rootNode.getElementsByTagName("day")
        for d in days:
            period = {}
            if d.hasAttribute("no"):
                day_no = d.getAttribute("no")
                pds = d.getElementsByTagName("period")
                for pd in pds:
                    period_info = {}
                    pd_no = pd.getAttribute("no")
                    hour = (pd.getElementsByTagName("hour")[0]).childNodes[0].data
                    hour = str(int(hour))
                    minute = (pd.getElementsByTagName("minute")[0]).childNodes[0].data
                    minute = str(int(minute))
                    hour = hour if len(hour) == 2 else '0' + hour
                    minute = minute if len(minute) == 2 else '0' + minute
                    h_m = hour + ":" + minute
                    # coord_type = (pd.getElementsByTagName("coord_type")[0]).childNodes[0].data
                    # control_type = (pd.getElementsByTagName("control_type")[0]).childNodes[0].data
                    max_cycle = (pd.getElementsByTagName("max_cycle")[0]).childNodes[0].data
                    min_cycle = (pd.getElementsByTagName("min_cycle")[0]).childNodes[0].data
                    plan_no = (pd.getElementsByTagName("plan_no")[0]).childNodes[0].data
                    period_info['no'] = pd_no
                    # period_info['coord_type'] = coord_type
                    # period_info['control_type'] = control_type
                    period_info['max_cycle'] = max_cycle
                    period_info['min_cycle'] = min_cycle
                    period_info['plan_no'] = plan_no
                    period[h_m] = period_info
            self.day[day_no] = period

    def read_plan_info(self):
        pls = self.rootNode.getElementsByTagName("plan")
        for pl in pls:
            if (pl.hasAttribute("no")):
                pl_no = pl.getAttribute("no")
                links_no = pl.getAttribute("links")
                #self.plan[pl_no] = links_no
                self.plan_phases[pl_no] = pl

    def add_day_no(self, sr):  # 流量加入day_no列
        month_day = sr['date'][5:11]
        week = sr['week']
        for key, value in self.schedule.items():
            start_month = str(int(value['start_month']))
            start_day = str(int(value['start_day']))
            end_month = str(int(value['end_month']))
            end_day = str(int(value['end_day']))

            start_month = start_month if len(start_month) == 2 else '0' + start_month
            start_day = start_day if len(start_day) == 2 else '0' + start_day
            end_month = end_month if len(end_month) == 2 else '0' + end_month
            end_day = end_day if len(end_day) == 2 else '0' + end_day
            start_date = start_month + "-" + start_day
            end_date = end_month + "-" + end_day
            if (month_day >= start_date and month_day <= end_date):
                if week in value['week']:
                    return str(int(value['day_no']))
        return ""

    def add_period_no(self, sr):
        day_info = self.day[sr['day_no']]
        period_no = ""
        for key, value in day_info.items():
            if sr['time'] >= key:
                period_no = str(int(day_info[key]['no']))
        return period_no

    def add_plan_no(self, sr):  # 流量加入plan_no列  TODO:day_info需要采用有序字典
        day_info = self.day[sr['day_no']]
        plan_no = ""
        for key, value in day_info.items():
            if sr['time'] >= key:
                plan_no = str(int(day_info[key]['plan_no']))
        return plan_no

    def add_cycle_length(self, sr):  # 增加最大周期和最小周期的限制
        """
        day_info = self.day[sr['day_no']]
        min_cycle, max_cycle = "", ""
        for key, value in day_info.items():
            if sr['time'] >= key:
                min_cycle = str(int(day_info[key]['min_cycle']))
                max_cycle = str(int(day_info[key]['max_cycle']))
        """
        min_cycle = 0
        for i in range(0, len(self.plan_para['phase_plan'])):
            min_green = self.plan_para['phase_plan'][i]['min_green']
            yellow = self.plan_para['phase_plan'][i]['yellow']
            all_red = self.plan_para['phase_plan'][i]['all_red']

            min_cycle += (min_green + yellow + all_red)
        min_cycle = max(min_cycle, self.plan_para['min_cycle'])
        max_cycle = self.plan_para['max_cycle']
        return pd.Series([min_cycle, max_cycle])

    def get_links_no(self, sr):  # 流量加入links_no列（渠化信息）
        return self.plan[sr['plan_no']]

    def caculate_flow(self, vehicle_flow, type=0):

        # if type==0:       #采用更新时间间隔内的过车，计算流量值
        flows = vehicle_flow.set_index('passtime').groupby(['camera_id', 'lane']).resample('%dT' % self.flow_interval)[
            'class'].count().reset_index(name='flow_rate')
        flows['flow_rate'] *= 60 / self.flow_interval
        flows_direction = vehicle_flow.set_index('passtime').groupby(['camera_id', 'lane', 'destination']).resample(
            '%dT' % self.flow_interval)[
            'class'].count().reset_index(name='direction_flow')
        flows_direction['direction_flow'] *= 60 / self.flow_interval

        flows_left = flows_direction[flows_direction['destination'] == 'left']
        flows_straight = flows_direction[flows_direction['destination'] == 'straight']
        flows_right = flows_direction[flows_direction['destination'] == 'right']
        flows_uturn = flows_direction[flows_direction['destination'] == 'uturn']

        flows_left = flows_left.drop(columns=['destination'])
        flows_straight = flows_straight.drop(columns=['destination'])
        flows_right = flows_right.drop(columns=['destination'])
        flows_uturn = flows_uturn.drop(columns=['destination'])

        flows = pd.merge(flows, flows_left, on=['camera_id', 'passtime', 'lane'], how='left').rename(
            columns=({'direction_flow': 'left'}))
        flows = pd.merge(flows, flows_straight, on=['camera_id', 'passtime', 'lane'], how='left').rename(
            columns=({'direction_flow': 'straight'}))
        flows = pd.merge(flows, flows_right, on=['camera_id', 'passtime', 'lane'], how='left').rename(
            columns=({'direction_flow': 'right'}))
        flows = pd.merge(flows, flows_uturn, on=['camera_id', 'passtime', 'lane'], how='left').rename(
            columns=({'direction_flow': 'uturn'}))

        flows['passtime'] = pd.to_datetime(flows['passtime'])
        flows['time'] = flows.apply(lambda x: x['passtime'].strftime("%H:%M"), axis=1)
        flows['date'] = flows['passtime'].dt.date.astype(str)
        flows['week'] = flows.apply(lambda x: x['passtime'].strftime("%w"), axis=1)
        self.flows = flows

    # 根据过车数据的时段，选取plan中对应的links编号；根据links编号，在detect_road中找links信息
    def read_phase_info(self, links_no):  # TODO，混合车道包含两个相位的问题（phase_lane)
        lights = self.rootNode.getElementsByTagName("light")  # 存在多个交叉口
        for light in lights:
            if light.hasAttribute("id") and light.getAttribute("id") == self.inter_id:  # 当前交叉口的编号
                all_links = light.getElementsByTagName("links")  # 存在多个links（可能存在可变车道或者潮汐车道情况）
                for all_link in all_links:
                    if all_link.hasAttribute("no") and all_link.getAttribute("no") == links_no:
                        links = all_link.getElementsByTagName("link")  # 当前渠化路段的编号
                        for link in links:
                            lane = link.getAttribute("fromLane")

                            if not link.hasAttribute("camera"):
                                continue

                            camera = int(link.getAttribute("camera"))

                            # 读取XML文件，返回路段-车道与相位的映射表
                            if link.hasAttribute("phase"):
                                phase = link.getAttribute("phase")
                                sat_flow = link.getAttribute("sat_flow")
                                if phase[0] == 'P':
                                    continue
                                if camera not in self.phase_lane.keys():
                                    self.phase_lane[camera] = {}
                                lane_info = {}
                                lane_info['phase'] = phase
                                lane_info['sat_flow'] = sat_flow
                                self.phase_lane[camera][lane] = lane_info

    # 添加路段编号
    def add_road_info(self):
        self.flows['road'] = self.flows['camera_id'].map(self.detect_road)

    def add_phase_no(self, sr):
        self.read_phase_info(sr['links_no'])  # 根据渠化编号，车道添加相位和饱和流率信息
        camera_lane_max = max(list(self.phase_lane[sr['camera_id']].keys()))    #当前摄像头拍摄的车道组的最大编号
        lane = str(int(camera_lane_max) - sr['lane'] + 1)
        if lane not in self.phase_lane[sr['camera_id']].keys():
            return pd.Series(['', ''])
        else:
            return pd.Series(
                [self.phase_lane[sr['camera_id']][lane]['phase'], self.phase_lane[sr['camera_id']][lane]['sat_flow']])

    # 根据起始时间和结束时间，读取对应时段的方案信息
    def read_traffic_plan(self, plan_no):
        plan_phase = self.plan_phases[plan_no]
        phases = {}

        rings = plan_phase.getElementsByTagName("ring")
        ring_size = len(rings)
        ring_list = [[] for i in range(0, ring_size)]
        i = 0
        for ring in rings:

            phases_info = ring.getElementsByTagName("state")
            for ph in phases_info:
                phase_info = {}
                phase_no = ph.getAttribute("phase")
                if ph.hasAttribute("overlap"):
                    overlap = ph.getAttribute("overlap")
                else:
                    overlap = ""
                green = ph.getAttribute("green")
                yellow = ph.getAttribute("yellow")
                all_red = ph.getAttribute("all_red")
                min_green = ph.getAttribute("min")
                max_green = ph.getAttribute("max")
                phase_info['overlap'] = overlap
                phase_info['green'] = green
                phase_info['yellow'] = yellow
                phase_info['all_red'] = all_red
                phase_info['min_green'] = min_green
                phase_info['max_green'] = max_green

                phase_info['split'] = int(green) + int(yellow) + int(all_red)
                phases[phase_no] = phase_info
                ring_list[i].append(phase_no)
            i += 1
        return ring_list, phases

    def ring_to_stage(self, ring_list, phases):  # TODO:搭接相位的主相位超过两个，相位在周期内服务超过1次
        # 将NEMA的环信息 转换为阶段相位方式（划分stage1,stage2,……)
        ##按照每个环的相位顺序，遍历每个的每个相位：
        ###纵向比较环中最小绿灯时长，并将该绿灯时长内的每个环相位存储为一个阶段，并编号；根据最小绿灯时长，当前阶段的相位都减去该值，作为下一次循环的相位绿灯。
        ###按照上述步骤，依次编号阶段。
        def judge_ring(col_increment, ring_list):
            for i in range(0, len(ring_list)):
                if (col_increment[i] < len(ring_list[i])):
                    return True
            return False

        col_increment = {}
        for i in range(0, len(ring_list)):
            col_increment[i] = 0

        stage = 1
        phase_stage = {}  # 存储相位对应的阶段
        stage_phase = {}  #存储阶段内的相位
        stage_info = {}
        while judge_ring(col_increment, ring_list):
            green_time = sys.maxsize
            for row_index in range(0, len(ring_list)):
                col_index = col_increment[row_index]
                if col_increment[row_index] < len(ring_list[row_index]):
                    other_phase = ring_list[row_index][col_index]
                    other_phase_time = phases[other_phase]['split']
                    green_time = min(green_time, other_phase_time)

            for row_index in range(0, len(ring_list)):
                if col_increment[row_index] >= len(ring_list[row_index]):
                    continue
                other_phase = ring_list[row_index][col_increment[row_index]]
                phase_stage[other_phase] = stage  # 插入阶段中的相位信息
                stage_value = {}
                stage_value['all_red'] = '0'
                stage_value['yellow'] = '0'
                stage_value['min_green'] = '0'
                stage_value['green_time'] = green_time
                stage_info[stage] = stage_value
                # 插入搭接相位，暂时除行人相位外
                overlap = phases[other_phase]['overlap']
                if overlap != "":
                    if "," in overlap:
                        list_phases = overlap.split(",")
                        for i in range(0, len(list_phases)):
                            if "P" not in list_phases[i]:
                                phase_stage[list_phases[i]] = stage
                    else:
                        phase_stage[overlap] = stage

                phases[other_phase]['split'] -= green_time
                if (phases[other_phase]['split'] == 0):
                    if (phases[other_phase]['all_red'] > stage_info[stage]['all_red']):
                        stage_info[stage]['all_red'] = phases[other_phase]['all_red']  # 添加阶段中，最大的全红时间
                    if (phases[other_phase]['yellow'] > stage_info[stage]['yellow']):
                        stage_info[stage]['yellow'] = phases[other_phase]['yellow']  # 添加阶段中，最大的全红时间
                    if (phases[other_phase]['min_green'] > stage_info[stage]['min_green']):
                        stage_info[stage]['min_green'] = phases[other_phase]['min_green']  # 添加阶段中，最大的全红时间
                    col_increment[row_index] += 1  # 更新环中的遍历序号
            stage += 1

            for key in phase_stage.keys():
                if phase_stage[key] not in stage_phase.keys():
                    stage_phase[phase_stage[key]] = set()
                stage_phase[phase_stage[key]].add(key)

            for key in stage_info.keys():
                stage_info[key]['id'] = list(stage_phase[key])
                stage_info[key]['pedestrian_time'] = 15

        return phase_stage, stage_info

    # 在相位基础上，将流量添加阶段信息和全红信息
    def add_stage_no_from_XML(self, sr):
        ring_list, phases = self.read_traffic_plan(sr['plan_no'])
        phase_stage, stage_info = self.ring_to_stage(ring_list, phases)
        stage_no, all_red, yellow, min_green = '', '', '', ''
        if sr['phase'] != '':
            stage_no = phase_stage[sr['phase']]
            if stage_no != '':
                all_red = stage_info[stage_no]['all_red']
                yellow = stage_info[stage_no]['yellow']
                min_green = stage_info[stage_no]['min_green']
        return pd.Series([stage_no, all_red, yellow, min_green])

    # 根据传入的阶段数组，添加阶段编号，全红，黄灯，最小绿信息
    def read_stage_phase_from_input(self):
        phase_plan = self.plan_para['phase_plan']
        length = len(phase_plan)
        for i in range(0, length):
            phase_list = phase_plan[i]['id']
            for j in range(0, len(phase_list)):
                self.stage_phases[phase_list[j]] = 'P' + str(i + 1)

    def add_stage_no_from_input(self, sr):
        phase_plan = self.plan_para['phase_plan']
        stage_no, all_red, yellow, min_green = '', '', '', ''
        if sr['phase'] != '':
            stage_no = self.stage_phases[sr['phase']]
            if stage_no != '':
                stage_index = int(stage_no.split('P')[1]) - 1
                all_red = phase_plan[stage_index]['all_red']
                yellow = phase_plan[stage_index]['yellow']
                min_green = phase_plan[stage_index]['min_green']
        return pd.Series([stage_no, all_red, yellow, min_green])

    # 调用traffic_timing算法
    ##traffic_timing算法里屏蔽聚类算法（timing_cluster）

    def read_stage(self):
        self.read_plan_info()
        #默认选第一个相位
        plan_nos = list(self.plan_phases.keys())
        phase_plan = []
        if len(plan_nos) > 0:
            ring_list, phases = self.read_traffic_plan(plan_nos[0])
            _,stage_info = self.ring_to_stage(ring_list, phases)
            for key in stage_info.keys():
                stage_info[key]['all_red'] = int(stage_info[key]['all_red'])
                stage_info[key]['yellow'] = int(stage_info[key]['yellow'])
                stage_info[key]['min_green'] = int(stage_info[key]['min_green'])
                stage_info[key]['pedestrian_time'] = int(stage_info[key]['pedestrian_time'])
                stage_info[key]['green_time'] = int(stage_info[key]['green_time'])
                phase_plan.append(stage_info[key])
        return phase_plan

    def generate_flow(self):
        vehicle_flow = self.read_data()  # 读取指定交叉口的过车数据
        self.caculate_flow(vehicle_flow, type=0)  # 统计流量和转向流量
        self.add_road_info()
        self.read_XML()
        self.read_stage_phase_from_input()  # 将阶段的信息生成为相位-阶段的映射表
        self.flows['day_no'] = self.flows.apply(self.add_day_no, axis=1)  # 增加日计划编号信息
        self.flows['period_no'] = self.flows.apply(self.add_period_no, axis=1)  # 增加时段编号信息
        self.flows['plan_no'] = self.flows.apply(self.add_plan_no, axis=1)  # 增加方案编号信息
        self.flows['links_no'] = self.links_no  # self.flows.apply(self.get_links_no,axis=1)   #增加渠化信息
        self.flows[['phase', 'sat_flow']] = self.flows.apply(self.add_phase_no, axis=1)  # 增加相位编号信息
        # self.flows[['stage_no','all_red','yellow','min_green']] = self.flows.apply(lambda x:self.add_stage_no_from_XML(x), axis=1)   #增加阶段编号信息,阶段的黄灯，阶段的最小率
        self.flows[['stage_no', 'all_red', 'yellow', 'min_green']] = self.flows.apply(
            lambda x: self.add_stage_no_from_input(x), axis=1)  # 增加阶段编号信息,阶段的黄灯，阶段的最小率
        self.flows[['min_cycle', 'max_cycle']] = self.flows.apply(lambda x: self.add_cycle_length(x),
                                                                  axis=1)  # 增加周期长度限制
        print('1、过车数据处理完成')