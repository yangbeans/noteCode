
把自定义python函数接口封装成http接口

模块说明：
    custom_services.py      自定义的服务程序函数模块
    runserver.py            把自定义函数封装成http服务接口并上线的主函数入口
    stop_forecast.py        停止程序工具                                例：bash stop_forecast.sh runserver

注意：
    stop_forecast.py 后边跟的文件名不能带".py"
    自定义函数模块主函数返回值类型必须是json格式，即'{"result":xx,...}'
    端口号范围1~65535，其中，1~1023为系统端口号，一般不使用       