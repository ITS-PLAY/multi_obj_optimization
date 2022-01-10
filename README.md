## 说明

根据输入的文件和方案信息，计算相位时长，并返回方案信息。



## 依赖环境

Python： 版本3.9

## 依赖包

pandas :  1.3.0

numpy：1.21.0

sklearn： 0.0



## 运行说明

main.py程序里

1、计算相位方案的函数：

```
plan_no,cycle,phase_plan = generate_traffic_time(data_file,traffic_light_file,phase_plan,inter_id)
```

```python
"""
参数：
    data_file：过车数据文件文件名，包含路径信息。
    traffic_light_file：优化前信控文件名，包含路径信息。
    phase_plan：后端传入的方案信息，包含相位编号、最小绿灯、黄灯时长、全红时长、行人时长。
    inter_id：交叉口的编号。
返回值：
    元组，包含三个参数。
    plan_no：优化前信控文件里对应时段的方案编号
    cycle：周期时长。
    phase_plan：方案信息，包含相位编号、最小绿灯、黄灯时长、全红时长、行人时长、相位时长。
"""
```

2、写入XML函数：

```
write_xml.write_plan_xml(plan_no, str(cycle), phase_plan, traffic_light_file, inter_id)
```

```python
"""
参数：
    plan_no：优化前的信控文件里对应时段的方案编号。
    cycle：周期长度。
    phase_plan：计算的方案信息，包含相位编号、最小绿灯、黄灯时长、全红时长、行人时长、阶段（相位）时长。
    traffic_light_file：优化前信控文件名，包含路径信息。
    inter_id：交叉口的编号。
返回值：
    无。
"""
```

