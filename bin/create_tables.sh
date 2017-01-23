#!/usr/bin/env bash

create_namespace 'face'

#创建人员特征码信息表
create 'face:id_code', 'f'

#创建民族编码表
create 'face:nation','num'

#创建人员信息民族对照表
create 'face:person_nation','num'

#创建重点人员信息表
create 'face:key_person_info','message'
