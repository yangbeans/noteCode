APP_NAME=$1 #forecast_runserver0001
PY_NAME=$APP_NAME\.py
#PID  代表是PID文件
PID=$APP_NAME\.pid

#使用说明，用来提示输入参数
usage() {
    echo "Usage: sh 执行脚本.sh [start|stop|restart|status]"
    echo "	start  启动应用"
    echo "	stop  停止运行"
    echo "	restart  重新启动，先停止，再启动"
    echo "	status  查询程序运行状态"
    exit 1
}

#检查程序是否在运行
is_exist(){
  pid=`ps -ef|grep $PY_NAME|grep -v grep|awk '{print $2}' `
  #如果不存在返回1，存在返回0     
  if [ -z "${pid}" ]; then
   return 1
  else
    return 0
  fi
}

#启动方法
start(){
  is_exist
  if [ $? -eq "0" ]; then         
    echo ">>> ${APP_NAME} is already running PID=${pid} <<<" 
  else 
    nohup python3 -u $PY_NAME > $APP_NAME\log\.log 2>&1 & 
    echo $! > $PID
    echo ">>> start $APP_NAME successed PID=$!          <<<"
   fi
  }


#停止方法
stop(){
  #is_exist
  pidf=$(cat $PID)
  #echo "$pidf"  
  echo ">>> ${APP_NAME} PID = $pidf begin kill $pidf    <<<"
  kill $pidf
  rm -rf $PID
  sleep 2
  is_exist
  if [ $? -eq "0" ]; then 
    echo ">>> $APP_NAME 2 PID = $pid begin kill -9 $pid  <<<"
    kill -9  $pid
    sleep 2
    echo ">>> $APP_NAME process stopped                  <<<"
  else
    echo ">>> $APP_NAME is not running                   <<<"
  fi  
}

#输出运行状态
status(){
  is_exist
  if [ $? -eq "0" ]; then
    echo ">>> ${APP_NAME} is running PID is ${pid} <<<"
  else
    echo ">>> $APP_NAME is not running             <<<"
  fi
}

#重启
restart(){
  stop
  start
}

#根据输入参数，选择执行对应方法，不输入则执行使用说明
case "stop" in
  "start")
    start
    ;;
  "stop")
    stop
    ;;
  "status")
    status
    ;;
  "restart")
    restart
    ;;
  *)
    usage
    ;;
esac
exit 0

