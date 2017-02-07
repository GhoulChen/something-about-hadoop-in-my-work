# something-about-hadoop-in-my-work




GBKOutPutFormat

    输出编码使用gbk编码

SortValue

    默认分组是以key做分组，value中不做排序，在部分计算中需要对value进行排序，使用自定义的Partition和Comparator，做到以skey.frist做分组，以skey.second为排序基准，使value有序化
