# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 10:29:44 2021

@author: yangsenbin

    自定义编写的服务程序
"""

#实现自定义的功能函数
class AddCustomService1:
    def my_add(self, data):
        a = float(data["a"])
        b = float(data["b"])
        r = a + b
        return '{"result", %s}'%r
    
