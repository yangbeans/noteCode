# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 10:08:39 2021

@author: yangsenbin

http服务api入口程序
"""

from tornado.options import define, options
from tornado.ioloop import IOLoop
from tornado import httpserver

# Applications  urls
from tornado.web import Application
from tornado.routing import ReversibleRuleRouter
from tornado.web import url

import os
base_path = os.path.dirname(__file__) #获取当前执行这个.py脚本的根目录

# DemoHandler  
import tornado.web
from concurrent.futures import ThreadPoolExecutor
from custom_services import AddCustomService1
import json
import sys
from tornado import gen
from tornado.concurrent import run_on_executor


#应用服务程序调用入口  
class Applications(Application, ReversibleRuleRouter):
    def __init__(self):
        #url(r"/httpdemo1", DemoHandler, name="httpdemo1")   "/httpdemo1"-->虚拟地址   
        
        handlers = [
            url(r"/httpdemo1", DemoHandler, name="httpdemo1"),
        ]
        super(Applications, self).__init__(handlers=handlers, **setting)
        

# 调用自定义服务程序主函数的类    views
class DemoHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(3)
    
    # 自己编写的应用程序类
    acs = AddCustomService1()
    
    @gen.coroutine
    def post(self, *args, **kwargs):
        result = yield self.forecast()
        self.write(result)
        self.finish()
        
    @run_on_executor
    def forecast(self):
        try:
            input_ = json.loads(self.get_body_argument("data"))   #将json字符串形式转化为字典
            #self.plogger_.info("input_:%s"%input_)
            output_ = self.acs.my_add(input_)
            # print(list(y_pred))   
            return output_
        except Exception as e:
            s=sys.exc_info()
            #print ("Error '%s' happened on line %d" % (s[1],s[2].tb_lineno))
            return '{"code":-1,"msg":"缺少必传参数，请检查"}'



# 系统参数设置
setting = {
    "debug": False,  ###!!!开多进程时记得把这个参数改为False
    "static_path": os.path.join(base_path, "static"),
    "template_path": os.path.join(base_path, "templates")
}

#port_ = sys.argv[1]
#port_ = int(port_)
# 设置端口号
port_ = 16002  #端口号  范围1~65535，其中，1~1023为系统端口号，一般不使用
define("port", default=port_, type=int)
define("address", default="0.0.0.0", type=str)


if __name__ == '__main__':
    options.parse_command_line()
    app = Applications()
    
    #开启预测多进程
    http_server = httpserver.HTTPServer(app)
    http_server.bind(options.port, options.address)
    http_server.start(num_processes=4)  # tornado将按照cpu核数来fork进程
    IOLoop.instance().start()
    print("done!")

