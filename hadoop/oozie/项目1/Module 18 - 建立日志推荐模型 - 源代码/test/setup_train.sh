#!/bin/bash

echo "hdfs dfs -test -d /user/events/workspace/train/scripts"
hdfs dfs -test -d /user/events/workspace/test/scripts
if [ $? == 0 ]; then
  echo "  hdfs dfs -rm -R -f -p /user/events/workspace/test"
  hdfs dfs -rm -R -f -skipTrash /user/events/workspace/test
fi
echo "  hdfs dfs -mkdir -p /user/events/workspace/test/scripts"
hdfs dfs -mkdir -p /user/events/workspace/test/scripts

echo "hdfs dfs -put -f test_event.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/1.test_event.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f user_friend.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/2.user_friend.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f event_attendee.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/3.event_attendee.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f event_creator_is_friend.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/4.event_creator_is_friend.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f user_attend_status.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/5.user_attend_status.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f friend_attend_status.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/6.friend_attend_status.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f friend_attend_percentage.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/7.friend_attend_percentage.hql /user/events/workspace/test/scripts

echo "hdfs dfs -put -f train_data.hql /user/events/workspace/test"
hdfs dfs -put -f ./scripts/8.train_data.hql /user/events/workspace/test/scripts


echo "hdfs dfs -test -d /user/events/workspace/test/scripts"
hdfs dfs -test -d /user/events/workspace/test/conf
if [ $? -ne 0 ]
then
  echo "  hdfs dfs -mkdir -p /user/events/workspace/test/conf"
  hdfs dfs -mkdir -p /user/events/workspace/test/conf
fi

# echo "hdfs dfs -put -f /usr/hdp/current/hive-client/conf/hive-site.xml /user/events/workspace/test/conf"
hdfs dfs -put -f /usr/hdp/current/hive-client/conf/hive-site.xml /user/events/workspace/test/conf

echo "hdfs dfs -put -f workflow.xml /user/events/workspace/test"
hdfs dfs -put -f workflow.xml /user/events/workspace/test

