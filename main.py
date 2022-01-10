from algorithm import write_xml, traffic_timing, traffic_flow

##示例文件
data_file = "./data/3车流-过车数据CSV.csv"
traffic_light_file = "./data/2信控.traffic_light.xml"
inter_id = "cluster_1083998784_3978846542"

phase_plan = [
    {'id': ['2', '6'],
     'min_green': 15,
     'yellow': 3,
     'all_red': 0,
     'pedestrian_time':15},

    {'id': ['1', '5'],
     'min_green': 15,
     'yellow': 3,
     'all_red': 0,
    'pedestrian_time':15},

    {'id': ['4', '8'],
     'min_green': 15,
     'yellow': 3,
     'all_red': 0,
    'pedestrian_time':15},

    {'id': ['3', '7'],
     'min_green': 15,
     'yellow': 3,
     'all_red': 0,
    'pedestrian_time':15}
]

plan_para ={
            'goal':1,
            'max_cycle': 150,
            'min_cycle': 60,
            'step':2,
            'phase_plan':phase_plan}

def generate_traffic_time(data_file,traffic_light_file,plan_para,inter_id):
    """
        生成配时方案的函数.
        参数：
            data_file：过车数据文件文件名，包含路径信息。
            traffic_light_file：优化前信控文件名，包含路径信息。
            plan_para：方案信息，包含最大周期、最小周期以及方案信息（相位编号、最小绿灯、黄灯时长、全红时长、行人时长）等。
            inter_id：交叉口的编号。
        返回值：
            元组，包含三个参数。
            plan_no：优化前信控文件里对应时段的方案编号。
            cycle：周期时长。
            plan_para：方案信息，包含最大周期、最小周期以及方案信息（相位编号、最小绿灯、黄灯时长、全红时长、行人时长、阶段（相位）时长）等。
    """

    vehicle_flow = traffic_flow.Traffic_Flow(data_file, traffic_light_file, plan_para, inter_id)
    vehicle_flow.generate_flow()

    traffic_time = traffic_timing.TrafficTiming(vehicle_flow.flows, traffic_light_file, plan_para, inter_id)
    traffic_time.auto_timing()
    return traffic_time.return_phase_plan()         #包含相位编号，周期时长，阶段信息

if __name__ == '__main__':
    plan_no,cycle,plan_para = generate_traffic_time(data_file,traffic_light_file,plan_para,inter_id)
    write_xml.write_plan_xml(plan_no, str(cycle), plan_para, traffic_light_file, inter_id)

