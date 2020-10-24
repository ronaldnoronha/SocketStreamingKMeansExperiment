ps aux | grep python3 | while read line
do
  array=($line)
  if [ ${array[10]} == "python3" -a ${array[11]} == "./monitor.py" ] ; then
    kill -9 ${array[1]}
  fi
done
