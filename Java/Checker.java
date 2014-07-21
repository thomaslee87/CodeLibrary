package com.taobao.scg.manager.cloudsync.checker;

import java.util.Date;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.taobao.common.dao.persistence.exception.DAOException;
import com.taobao.scg.admin.dao.HiveTableDAO;
import com.taobao.scg.admin.dao.UdcJobListDAO;
import com.taobao.scg.admin.data.model.UdcJobListDO;
import com.taobao.scg.admin.data.model.UdcScgHiveTableDO;
import com.taobao.scg.admin.data.model.UdcScgHiveTableDomain;
import com.taobao.scg.manager.cloudsync.DataxPluginEnum;
import com.taobao.scg.manager.cloudsync.SkyParamEnum;
import com.taobao.scg.manager.cloudsync.SyncConstants;
import com.taobao.scg.manager.cloudsync.creater.DefaultJobXmlCreater;
import com.taobao.scg.manager.cloudsync.creater.JobXml;
import com.taobao.util.CollectionUtil;
/**
 * @author yangyu.xcf
 * 用于检测数据有无产出
 */


public abstract  class Checker  implements Observer {
    protected  Logger logger = LoggerFactory.getLogger(Checker.class);
    
    @Autowired
    CheckerObservable checkerObservable;
    @Autowired
    protected UdcJobListDAO udcJobListDAO;
    @Resource
    protected HiveTableDAO ibatisHiveTableDAO;
    @Autowired
    protected DefaultJobXmlCreater defaultJobXmlCreater;
//  @Autowired
//  private CreaterParamGetter createrParamGetter;
    
    
    /** 
     * 根据@UdcScgHiveTableDO检测数据是否已经ready
     * @param udcScgHiveTableDO
     * @return  true 数据准备OK   false 数据还没准备
     */
     public abstract boolean checkData(UdcScgHiveTableDO udcScgHiveTableDO)throws Exception;
    
    
    /**
     * 根据元数据检测数据是否产出,若已经产出,则添加job到jobList表中
     * 方法给天网,HDFS,以及定时使用
     * @param @UdcScgHiveTableDO 元数据
     * @return jobId:插入成功,返回jobId
     *          0:job已经存在,重复插入(状态相同,名称相同),
     *          -1 数据还未产出
     * @throws Exception  所有异常全部往上抛
     */
    public long checkAndAddJob(UdcScgHiveTableDO udcScgHiveTableDO) throws Exception{
        if(this.checkData(udcScgHiveTableDO)){//检测数据是否已经产出
            String jobName = SkyParamEnum.gmtdate.getTimeStr(new Date())+//今天日期
            SyncConstants.JOB_NAME_SPLIT+//|分隔符
                udcScgHiveTableDO.getTableName();//表名
            jobName = this.appendParams(jobName,udcScgHiveTableDO);//产出jobName
            long jobId = this.insertBasicJobList(jobName,1,udcScgHiveTableDO.getTableType());//首先插入基本信息的job
            if(jobId>0){
                 this.addXmlToJob(jobName,jobId,udcScgHiveTableDO);//添加XML到JOB
                 return jobId;
            }
            return 0;
        }
        return -1;
    }
    
    /** 
     * 根据jobName添加job,数据确定产出方式为notify
     * @param udcScgHiveTableDO
     * @param jobName 根据notify内容组装的jobId
     * @return  jobId:插入成功,返回jobId
     *          0:job已经存在,重复插入(状态相同,名称相同),
     *          -1:异常
     */
    public long addJobList(String jobName){
        try {
            long jobId = this.insertBasicJobList(jobName,2,-1); //这里先插入到jobList中保证http请求不因异常遗漏
            if(jobId>0){
                String hiveTable = CloudSyncUtil.getHiveTableByJobName(jobName);
                UdcScgHiveTableDO udcScgHiveTableDO = new UdcScgHiveTableDO();
                udcScgHiveTableDO.setTableName(hiveTable);
                List<UdcScgHiveTableDO>  udcScgHiveTableDOList= ibatisHiveTableDAO.getByQuery(udcScgHiveTableDO);
                if(CollectionUtil.isNotEmpty(udcScgHiveTableDOList)){
                    this.addXmlToJob(jobName,jobId, udcScgHiveTableDOList.get(0));  //添加XML到JOB
                }
                
                UdcJobListDO udcJobListDO = new UdcJobListDO();
                udcJobListDO.setJobid(jobId);
                if(udcScgHiveTableDOList.get(0).getTableType() == SyncConstants.ScgHiveTableTableType.yun2Table.getTableType())
                    udcJobListDO.setJobType("datax-yun2");
                else
                    udcJobListDO.setJobType("datax");
                udcJobListDAO.updateUdcJobList(udcJobListDO);
            }
            return jobId;
        } catch (Exception e) {
            logger.error("http请求 添加任务失败", e);
        }
        return -1;
    }
    
    
    

     

    /** 
     * 根据jobName jobId 以及元数据信息 产出jobxml 并添加到job中去
     * @param udcScgHiveTableDO
     * @param jobName 任务名称
     * @param jobId 任务id
     * @return  true xml添加成功
     *          false xml添加失败
     * @throws Exception 
     */
    public boolean addXmlToJob(String jobName,Long jobId ,UdcScgHiveTableDO udcScgHiveTableDO) throws Exception{
        try {
            UdcJobListDO  udcJobListDO = new UdcJobListDO();
            udcJobListDO.setJobid(jobId);
            udcJobListDO.setFeature(this.createJobXml( jobName, jobId, udcScgHiveTableDO));//产出jobXml
            return udcJobListDAO.updateUdcJobList(udcJobListDO)==1?true:false;
        } catch (Exception e) {
            logger.error("产生xml并添加到JOB失败", e);
            throw new Exception();
        }
    }
    
    /**
     * 根据jobName jobId 以及元数据信息 产出jobxml 
     * 
     */
    public String createJobXml(String jobName,long jobId,UdcScgHiveTableDO udcScgHiveTableDO) throws InstantiationException, IllegalAccessException, DAOException {
        UdcScgHiveTableDomain metaDomain = new UdcScgHiveTableDomain();
        metaDomain.setHtDO(udcScgHiveTableDO);
        metaDomain.setBizParamsMap(CloudSyncUtil.getBizParamsByJobName(jobName));
        
        defaultJobXmlCreater.init(
                DataxPluginEnum.getReaderByDbype(udcScgHiveTableDO.getTableType())
                    ,DataxPluginEnum.writer.udchbasewriter);//初始化一个jobXml  creater
        defaultJobXmlCreater.setJobId(jobId);//设置任务名称
        JobXml jobXml = defaultJobXmlCreater.createJobXml(metaDomain);//添加pulgin的参数
        return jobXml.getDocument().asXML();
    }
    
    /**
     * 根据元数据信息@UdcScgHiveTableDO 将可能出现的参数添加到jobName后面,参数可以出现在TablePath或者FilterSql中
     * @param jobName 需要添加参数的job名称
     * @param @UdcScgHiveTableDO 元数据信息
     * @return 完整的jobName 
     */
    protected String appendParams(String jobName,UdcScgHiveTableDO udcScgHiveTableDO) {
        //以下2个字段可能出现参数TablePath或者FilterSql
        Set<String> paramSet = new TreeSet<String>();
        if(StringUtils.isNotEmpty(udcScgHiveTableDO.getTablePath())){
            for(SkyParamEnum p :SkyParamEnum.values()){
                if(udcScgHiveTableDO.getTablePath().indexOf("${"+p.name()+"}")>0){
                    paramSet.add(p.name());
                }
            }
        }
        if(StringUtils.isNotEmpty(udcScgHiveTableDO.getFilterSql())){
            for(SkyParamEnum p :SkyParamEnum.values()){
                if(udcScgHiveTableDO.getFilterSql().indexOf("${"+p.name()+"}")>0){
                    paramSet.add(p.name());
                }
            }
        }
        for(String p :paramSet){
            jobName+=SyncConstants.JOB_NAME_SPLIT.concat(p).concat(SyncConstants.JOB_NAME_PARAM_SPLIT).concat(SkyParamEnum.valueOf(p).getTimeStr(new Date()));
        }
        return jobName;
    }
    
    /**根据jobName插入一个基本的dataxjob,如果是重复的记录,那么不插入
     * 这个方法应该属于manager层的方法,因为没有该层,所以先写在这里
     * @param  jobName任务名称
     * @param jobType  1:scan出来的job, 2 http发回来的job (两种类型对于重复记录的定义不一样,scan只要今天被scan过就不插入了,http的则不管,只需检测job状态)
     * @return jobId 任务Id
     * @throws DAOException
     */
    private long insertBasicJobList(String jobName,int jobType,int tableType) throws DAOException{
        UdcJobListDO udcJobListDO = new UdcJobListDO();
        udcJobListDO.setJobName(jobName);
        if(jobType==2){
            udcJobListDO.setJobStatus(0);
        }
        if(tableType==SyncConstants.ScgHiveTableTableType.yun2Table.getTableType()){
            udcJobListDO.setJobType("datax-yun2");
        }
        else{
            udcJobListDO.setJobType("datax");
        }
//      udcJobListDO.setJobType("datax");
        udcJobListDO.setResourceGroup("udc_datax");
        List<UdcJobListDO> udcJobListDOList = udcJobListDAO.queryUdcJobList(udcJobListDO);
        if(CollectionUtil.isNotEmpty(udcJobListDOList)&&udcJobListDOList.size()>0){
            return 0;
        }
        udcJobListDO.setJobStatus(0);//scan出来的job 在这里也加入状态
        return udcJobListDAO.insertUdcJobList(udcJobListDO);
    }
    

    //重写equals 方法,为了在添加观察者的时候去重
    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Checker)&&(obj.getClass().getName().equals(this.getClass().getName()));
    }
    
    @Override
    public void update(Observable o, Object arg) {
         CheckerStatistics cs = (CheckerStatistics)arg;
         try{//异常说明,一种任务出错了就跳到下一种检测方式,因为一种任务中的一个出错了表名其他的也有极大可能出错
             UdcScgHiveTableDO udcScgHiveTableDO = new UdcScgHiveTableDO();
             udcScgHiveTableDO.setRetType(this.getDbRealType());//1:天网节点;2:定时产出;3:文件确认
             List<UdcScgHiveTableDO> htDOList = ibatisHiveTableDAO.getByQuery(udcScgHiveTableDO);
             if(CollectionUtil.isNotEmpty(htDOList)){
                 long checkRs ;
                 for(UdcScgHiveTableDO h :htDOList){
                     if(CheckerObservable.jFButton&&CheckerObservable.getJobFileter().contains(h.getTableName())){//这个元数据今天已经被检测过了 ,直接跳过
                         continue;
                     }
                     if((checkRs= this.checkAndAddJob(h))>=0){//检测成功,表名数据已经产出
                         synchronized (this) {
                             CheckerObservable.addJobFileter(h.getTableName());//检测成功,并成功新增或已存在,那么就加入到不检测列表中区去
                        }
                         if(checkRs>0){
                             cs.addCheckedCount();//检测成功 并新增的数量
                         }
                     }
                 }
             }
         }
         catch(Exception e){
             logger.error(e.getMessage(),e);
             cs.addErrorType(this.getErrorType());
         }
    }

    public abstract int getDbRealType();
    public abstract int getErrorType();
    
}

