#! /usr/bin/bash
#------variables used------#
S="************************************"
D="-------------------------------------"
COLOR="y"

file_path="./healthlogs.txt"; size=$(stat -c%s "$file_path"); [ $size -gt 5000000 ] && rm "$file_path"

MOUNT=$(mount|egrep -iw "ext4|ext3|xfs|gfs|gfs2|btrfs"|grep -v "loop"|sort -u -t' ' -k1,2)
FS_USAGE=$(df -PThl -x tmpfs -x iso9660 -x devtmpfs -x squashfs|awk '!seen[$1]++'|sort -k6n|tail -n +2)
IUSAGE=$(df -iPThl -x tmpfs -x iso9660 -x devtmpfs -x squashfs|awk '!seen[$1]++'|sort -k6n|tail -n +2)

if [ $COLOR == y ]; then
{
 GCOLOR="\e[47;32m ------ OK/HEALTHY \e[0m"
 WCOLOR="\e[43;31m ------ WARNING \e[0m"
 CCOLOR="\e[47;31m ------ CRITICAL \e[0m"
}
else
{
 GCOLOR=" ------ OK/HEALTHY "
 WCOLOR=" ------ WARNING "
 CCOLOR=" ------ CRITICAL "
}
fi

echo -e "$S"
echo -e "\tSystem Health Status"
echo -e "$S"

# truncating the file if it exceeded 5MB size
[ $(stat -c%s healthlogs.txt) -gt 5000000 ] && truncate -s 0 healthlogs.txt


#--------Print Operating System Details--------#
hostname -f &> /dev/null && printf "Hostname : $(hostname -f)" || printf "Hostname : $(hostname -s)"

echo -en "\nOperating System : "
[ -f /etc/os-release ] && echo $(egrep -w "NAME|VERSION" /etc/os-release|awk -F= '{ print $2 }'|sed 's/"//g') || cat /etc/system-release

echo -e "Kernel Version :" $(uname -r)
printf "OS Architecture :"$(arch | grep x86_64 &> /dev/null) && printf " 64 Bit OS\n"  || printf " 32 Bit OS\n"

#--------Print system uptime-------#
UPTIME=$(uptime)
echo -en "System Uptime : "
echo $UPTIME|grep day &> /dev/null
if [ $? != 0 ]; then
  echo $UPTIME|grep -w min &> /dev/null && echo -en "$(echo $UPTIME|awk '{print $2" by "$3}'|sed -e 's/,.*//g') minutes" \
 || echo -en "$(echo $UPTIME|awk '{print $2" by "$3" "$4}'|sed -e 's/,.*//g') hours"
else
  echo -en $(echo $UPTIME|awk '{print $2" by "$3" "$4" "$5" hours"}'|sed -e 's/,//g')
fi
echo -e "\nCurrent System Date & Time : "$(date +%c)

#--------Check for any read-only file systems--------#
echo -e "\nChecking For Read-only File System[s]"
echo -e "$D"
echo "$MOUNT"|grep -w ro && echo -e "\n.....Read Only file system[s] found"|| echo -e ".....No read-only file system[s] found. "

#--------Check for currently mounted file systems--------#
echo -e "\n\nChecking For Currently Mounted File System[s]"
echo -e "$D$D"
echo "$MOUNT"|column -t

#--------Check disk usage on all mounted file systems--------#
echo -e "\n\nChecking For Disk Usage On Mounted File System[s]"
echo -e "$D$D"
echo -e "( 0-85% = OK/HEALTHY,  85-95% = WARNING,  95-100% = CRITICAL )"
echo -e "$D$D"
echo -e "Mounted File System[s] Utilization (Percentage Used):\n"

COL1=$(echo "$FS_USAGE"|awk '{print $1 " "$7}')
COL2=$(echo "$FS_USAGE"|awk '{print $6}'|sed -e 's/%//g')

for i in $(echo "$COL2"); do
{
  if [ $i -ge 95 ]; then
    COL3="$(echo -e $i"% $CCOLOR\n$COL3")"
  elif [[ $i -ge 85 && $i -lt 95 ]]; then
    COL3="$(echo -e $i"% $WCOLOR\n$COL3")"
  else
    COL3="$(echo -e $i"% $GCOLOR\n$COL3")"
  fi
}
done
COL3=$(echo "$COL3"|sort -k1n)
paste  <(echo "$COL1") <(echo "$COL3") -d' '|column -t

#--------Check for any zombie processes--------#
echo -e "\n\nChecking For Zombie Processes"
echo -e "$D"
ps -eo stat|grep -w Z 1>&2 > /dev/null
if [ $? == 0 ]; then
  echo -e "Number of zombie process on the system are :" $(ps -eo stat|grep -w Z|wc -l)
  echo -e "\n  Details of each zombie processes found   "
  echo -e "  $D"
  ZPROC=$(ps -eo stat,pid|grep -w Z|awk '{print $2}')
  for i in $(echo "$ZPROC"); do
      ps -o pid,ppid,user,stat,args -p $i
  done
else
 echo -e "No zombie processes found on the system."
fi

#--------Check Inode usage--------#
echo -e "\n\nChecking For INode Usage"
echo -e "$D$D"
echo -e "( 0-85% = OK/HEALTHY,  85-95% = WARNING,  95-100% = CRITICAL )"
echo -e "$D$D"
echo -e "INode Utilization (Percentage Used):\n"

COL11=$(echo "$IUSAGE"|awk '{print $1" "$7}')
COL22=$(echo "$IUSAGE"|awk '{print $6}'|sed -e 's/%//g')

for i in $(echo "$COL22"); do
{
  if [[ $i = *[[:digit:]]* ]]; then
  {
  if [ $i -ge 95 ]; then
    COL33="$(echo -e $i"% $CCOLOR\n$COL33")"
  elif [[ $i -ge 85 && $i -lt 95 ]]; then
    COL33="$(echo -e $i"% $WCOLOR\n$COL33")"
  else
    COL33="$(echo -e $i"% $GCOLOR\n$COL33")"
  fi
  }
  else
    COL33="$(echo -e $i"% (Inode Percentage details not available)\n$COL33")"
  fi
}
done

COL33=$(echo "$COL33"|sort -k1n)
paste  <(echo "$COL11") <(echo "$COL33") -d' '|column -t

#--------Check for SWAP Utilization--------#
echo -e "\n\nChecking SWAP Details"
echo -e "$D"
echo -e "Total Swap Memory in MiB : "$(grep -w SwapTotal /proc/meminfo|awk '{print $2/1024}')", in GiB : "\
$(grep -w SwapTotal /proc/meminfo|awk '{print $2/1024/1024}')
echo -e "Swap Free Memory in MiB : "$(grep -w SwapFree /proc/meminfo|awk '{print $2/1024}')", in GiB : "\
$(grep -w SwapFree /proc/meminfo|awk '{print $2/1024/1024}')

#--------Check for Processor Utilization (current data)--------#
echo -e "\n\nChecking For Processor Utilization"
echo -e "$D"
echo -e "\nCurrent Processor Utilization Summary :\n"
mpstat|tail -2

#--------Check for load average (current data)--------#
echo -e "\n\nChecking For Load Average"
echo -e "$D"
echo -e "Current Load Average : $(uptime|grep -o "load average.*"|awk '{print $3" " $4" " $5}')"

#------Print most recent 3 reboot events if available----#
echo -e "\n\nMost Recent 3 Reboot Events"
echo -e "$D$D"
last -x 2> /dev/null|grep reboot 1> /dev/null && /usr/bin/last -x 2> /dev/null|grep reboot|head -3 || \
echo -e "No reboot events are recorded."

#------Print most recent 3 shutdown events if available-----#
echo -e "\n\nMost Recent 3 Shutdown Events"
echo -e "$D$D"
last -x 2> /dev/null|grep shutdown 1> /dev/null && /usr/bin/last -x 2> /dev/null|grep shutdown|head -3 || \
echo -e "No shutdown events are recorded."

#--------Print top 5 Memory & CPU consumed process threads---------#
#--------excludes current running program which is hwlist----------#
echo -e "\n\nTop 5 Memory Resource Hog Processes"
echo -e "$D$D"
ps -eo pmem,pid,ppid,user,stat,args --sort=-pmem|grep -v $$|head -6|sed 's/$/\n/'

echo -e "\nTop 5 CPU Resource Hog Processes"
echo -e "$D$D"
ps -eo pcpu,pid,ppid,user,stat,args --sort=-pcpu|grep -v $$|head -6|sed 's/$/\n/'


# cron job/ crontab command
# 0 15 * * 1,3,7 /home/nileshtest/systemhealth/mainscript.sh >> /home/nileshtest/systemhealth/healthlogs.txt 2>&1
