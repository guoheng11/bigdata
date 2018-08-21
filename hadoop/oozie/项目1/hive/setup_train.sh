#!/bin/bash

echo "hdfs dfs -test -d /user/events/workspace/train/lib"
hdfs dfs -test -d /user/events/workspace/train/lib
if [ $? -ne 0 ]
then
echo "  hdfs dfs -mkdir -p /user/events/workspace/train/lib"
  hdfs dfs -mkdir -p /user/events/workspace/train/lib
fi

echo "hdfs dfs -put -f train_event.hql /user/events/workspace/train"
hdfs dfs -put -f 1.train_event.hql /user/events/workspace/train

echo "hdfs dfs -put -f user_friend.hql /user/events/workspace/train"
hdfs dfs -put -f 2.user_friend.hql /user/events/workspace/train

echo "hdfs dfs -put -f event_attendee.hql /user/events/workspace/train"
hdfs dfs -put -f 3.event_attendee.hql /user/events/workspace/train

echo "hdfs dfs -put -f event_creator_is_friend.hql /user/events/workspace/train"
hdfs dfs -put -f 4.event_creator_is_friend.hql /user/events/workspace/train

echo "hdfs dfs -put -f user_attend_status.hql /user/events/workspace/train"
hdfs dfs -put -f 5.user_attend_status.hql /user/events/workspace/train

echo "hdfs dfs -put -f friend_attend_status.hql /user/events/workspace/train"
hdfs dfs -put -f 6.friend_attend_status.hql /user/events/workspace/train

echo "hdfs dfs -put -f friend_attend_percentage.hql /user/events/workspace/train"
hdfs dfs -put -f 7.friend_attend_percentage.hql /user/events/workspace/train

echo "hdfs dfs -put -f train_data.hql /user/events/workspace/train"
hdfs dfs -put -f 8.train_data.hql /user/events/workspace/train


echo "hdfs dfs -put -f /usr/hdp/current/hbase-client/conf/hbase-site.xml /user/events/workspace/train"
hdfs dfs -put -f /usr/hdp/current/hbase-client/conf/hbase-site.xml /user/events/workspace/train

echo "hdfs dfs -put -f /usr/hdp/current/hive-client/conf/hive-site.xml /user/events/workspace/train"
hdfs dfs -put -f /usr/hdp/current/hive-client/conf/hive-site.xml /user/events/workspace/train
