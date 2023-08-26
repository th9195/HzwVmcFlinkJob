package com.hzw.fdc.engine.ext;


import java.util.List;

/**
 *  Indicator的历史数据接口
 */
public interface IIndicatorHistoryValueService {


    /**
     * 查询当前系统缓存的最大Indicator历史数量
     * @return 整数
     */
    public Integer getHistoryMaxCount(String indicatorId);

    /**
     * 根据指定大小查询当前系统缓存的指定的indicator的历史数据.
     * @param size  期望获取的indicator历史数据的数量。 如果传入的大小大于了已存储数量，则取两者最小的。
     * @return 返回一个有序的indicator历史数据，按IndicatorValue的timestamp字段倒序排序，即最新的在最前面，最老的数据在最后面。
     */
    public List<IndicatorValue> getHistoryValue(String indicatorId, int size);


    public static class IndicatorValue{
        public Double value;
        public Long timestamp;
    }
}
