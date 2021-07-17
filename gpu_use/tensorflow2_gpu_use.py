# -*- coding: utf-8 -*-
"""
Created on Mon May 10 15:20:49 2021

@author: yangsenbin
目标：tensorflow2.x版本使用gpu资源
    声明：机器要安装了tensorflow-gpu版本
        租赁gpu云服务器其实就是租赁一台带有gpu的（linux）服务器，其他操作（如代码调试、模型训练部署等）跟在平台linux服务器上操作是一样的
    
    参考：https://blog.csdn.net/weixin_43213607/article/details/108576900
    
    通常在训练的时候在代码前面套用以下代码即可
"""

import tensorflow as tf
import os
# 列出当前主机的物理显卡GPU或者CPU的列表
gpus = tf.config.experimental.list_physical_devices(device_type='GPU')
cpus = tf.config.experimental.list_physical_devices(device_type='CPU')
print("Num GPUs Available: ", len(gpus))

os.environ["CUDA_DEVICE_ORDER"] = 'PCI_BUS_ID'
os.environ['CUDA_VISIBLE_DEVICES'] = '0'

# 设置当前程序可见GPU设备
gpus = tf.config.experimental.list_physical_devices("GPU")

# 显卡内存的使用(动态申请的方策略)
if gpus:
    try:
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)
    except RuntimeError as e:
        print(e)
        exit(-1)
    
# tf相关代码，通过以上步骤，tensorflow2.x版本就能正常使用机器中的gpu资源了
#tensorflow coding.......