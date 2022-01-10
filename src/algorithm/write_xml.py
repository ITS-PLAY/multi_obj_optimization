# 采用起始和结束时间方式，保存成中间结构
from collections import Counter
import xml.etree.cElementTree as xee
import time

def write_plan_xml(plan_no, cycle, phase_plan, traffic_light_file, inter_id):
    """
        将阶段表示的方案，写入到XML文件中。
        参数：
            plan_no：优化前的信控文件里对应时段的方案编号。
            cycle：周期长度。
            phase_plan：计算的方案信息，包含相位编号、最小绿灯、黄灯时长、全红时长、行人时长、阶段（相位）时长。
            traffic_light_file：优化前信控文件名，包含路径信息。
            inter_id：交叉口的编号。
        返回值：
            无。
    """


    ring_num = 2
    ring_list = [[] for i in range(0, ring_num)]

    ring_col_index = {}
    for i in range(0, ring_num):
        ring_col_index[i] = 0

    overlap_phases = {}
    overlap = {}

    def list_to_str(detect_ids):
        id_string = ''
        for j in range(0, len(detect_ids)):
            id_string += "%s" % detect_ids[j] + ','
        id_string = id_string[:-1]
        return id_string

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
                ring_main_phase = [no for no in ring_phases_no[m] if int(no) % 2 == 0][0]
                overlap_p = [no for no in ring_phases_no[m] if int(no) % 2 == 1][0]
                if overlap_p in overlap_phases.keys():
                    overlap_phases[overlap_p].append(ring_main_phase)
                else:
                    overlap_phases[overlap_p] = [ring_main_phase]

            # 如果当前阶段的各环主相位编号与数组对应行的前一个元素的主相位编号不相同时，则将新建的字典添加到对应行数组中
            if len(ring_list[m]) == 0:
                dict_info = {'id': ring_main_phase, 'start': '0', 'end': str(stage_time), 'overlap': '', 'barrier': '0',
                             'yellow': str(yellow), 'all_red': str(all_red)}
                ring_list[m].append(dict_info)
            elif ring_main_phase != ring_list[m][ring_col_index[m]]['id']:
                dict_info = {'id': ring_main_phase, 'start': ring_list[m][ring_col_index[m]]['end'], 'end': '',
                             'overlap': '', 'barrier': '0', 'yellow': str(yellow), 'all_red': str(all_red)}
                dict_info['end'] = str(int(dict_info['start']) + stage_time)
                ring_list[m].append(dict_info)
                ring_col_index[m] += 1
            else:
                # 如果当前阶段的各环主相位编号与数组对应行的前一个元素的主相位编号相同，则直接更新前一个元素的结束时间 += 阶段时长
                ring_list[m][ring_col_index[m]]['end'] = str(int(ring_list[m][ring_col_index[m]]['end']) + stage_time)
                ring_list[m][ring_col_index[m]]['yellow'] = str(yellow)
                ring_list[m][ring_col_index[m]]['all_red'] = str(all_red)

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
                    dict_info = {'id': '0', 'start': '0', 'end': '0', 'overlap': '', 'barrier': '1', 'yellow': '0',
                                 'all_red': '0'}
                    ring_list[m].insert(0, dict_info)
                else:
                    ring_list[m][ring_col_index[m] - 1]['barrier'] = '1'

        # 或者遍历结束时，同时更新第一行和第二行的前一个元素的相位后屏障值；
        if i == len(phase_plan) - 1:
            for m in range(0, ring_num):
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
            if ring_list[m][n]['id'] in overlap.keys():
                ring_list[m][n]['overlap'] = list_to_str(overlap[ring_list[m][n]['id']])

    # 写入XML
    eTree = xee.parse(traffic_light_file)
    rootNode = eTree.getroot()
    lights = rootNode.findall('light')
    for light in lights:
        if (light.get('id') == inter_id):
            system_time_text = light.findall('system_time')
            system_time_text[0].text = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            plans = light.findall('plan')
            plans_from_xml = {}
            for plan in plans:
                if plan.get('no') == plan_no:
                    # plan = xee.SubElement(light,"plan",{'no':plan_no,'links'='1'})
                    cycle_text = plan.findall('cycle')
                    cycle_text[0].text = cycle

                    rings = plan.findall('ring')
                    for ring in rings:
                        plan.remove(ring)

                    for i in range(0, ring_num):

                        ring = xee.SubElement(plan, "ring")
                        for j in range(0, len(ring_list[m])):
                            if ring_list[i][j]['id'] == '0':
                                state = xee.SubElement(ring, 'barrier')
                                continue
                            state = xee.SubElement(ring, 'state', {'phase': ring_list[i][j]['id'],
                                                                   'overlap': ring_list[i][j]['overlap'],
                                                                   'green': str(int(ring_list[i][j]['end']) - int(
                                                                       ring_list[i][j]['start']) - int(
                                                                       ring_list[i][j]['yellow']) - int(
                                                                       ring_list[i][j]['all_red'])),
                                                                   'yellow': ring_list[i][j]['yellow'],
                                                                   'all_red': ring_list[i][j]['all_red'],
                                                                   'coord_status': '0',
                                                                   'min': '15',
                                                                   'max': '90'})
                            if ring_list[i][j]['barrier'] == '1':
                                state = xee.SubElement(ring, 'barrier')

    eTree.write('./xml/sample.xml', encoding="utf-8", xml_declaration=True)
    print("3、配时方案写入XML完成")