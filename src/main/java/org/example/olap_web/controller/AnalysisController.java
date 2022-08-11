package org.example.olap_web.controller;

import org.example.olap_web.dao.*;
import org.example.olap_web.pojo.*;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class AnalysisController {
    DAUDaoImpl dauDaoImpl = new DAUDaoImpl();
    DNUDaoImpl dnuDaoImpl = new DNUDaoImpl();
    TrafficDaoImpl trafficImpl = new TrafficDaoImpl();
    EventDaoImpl eventImpl = new EventDaoImpl();
    AdvertiseDaoImpl advertiseImpl = new AdvertiseDaoImpl();

    // TODO 1. 日活分析
    // TODO 1.1 省份维度日活数
    @RequestMapping("/user/provinceDAU")
    @CrossOrigin
    List getProvinceDAU(){
        List<DAUOneDim> provinceDauList = dauDaoImpl.getOneDimDauInfo("province");
        return provinceDauList;
    }

    // TODO 1.2 城市维度日活数
    @RequestMapping("/user/cityDAU")
    @CrossOrigin
    List getCityDAU(){
        List<DAUOneDim> cityDauList = dauDaoImpl.getOneDimDauInfo("city");
        return cityDauList;
    }

    // TODO 1.3 区县维度日活数
    @RequestMapping("/user/districtDAU")
    @CrossOrigin
    List getDistrictDAU(){
        List<DAUOneDim> districtDauList = dauDaoImpl.getOneDimDauInfo("district");
        return districtDauList;
    }

    // TODO 1.4 软件版本维度日活数
        @RequestMapping("/user/appverDAU")
    @CrossOrigin
    List getAppverDAU(){
        List<DAUOneDim> appverDauList = dauDaoImpl.getOneDimDauInfo("appver");
        return appverDauList;
    }

    // TODO 1.5 设备类型维度日活数
    @RequestMapping("/user/devicetypeDAU")
    @CrossOrigin
    List getAppverAU(){
        List<DAUOneDim> devicetypeDauList = dauDaoImpl.getOneDimDauInfo("devicetype");
        return devicetypeDauList;
    }


    // TODO 2. 日新分析
    // TODO 2.1 省份维度日新数
    @RequestMapping("/user/provinceDNU")
    @CrossOrigin
    List getProvinceDNU(){
        List<DNUOneDim> provinceDNUList = dnuDaoImpl.getOneDimDnuInfo("province");
        return provinceDNUList;
    }

    // TODO 2.2 城市维度日新数
    @RequestMapping("/user/cityDNU")
    @CrossOrigin
    List getCityDNU(){
        List<DNUOneDim> cityDNUList = dnuDaoImpl.getOneDimDnuInfo("city");
        return cityDNUList;
    }

    // TODO 2.3 区县维度日新数
    @RequestMapping("/user/districtDNU")
    @CrossOrigin
    List getDistrictDNU(){
        List<DNUOneDim> districtDNUList = dnuDaoImpl.getOneDimDnuInfo("district");
        return districtDNUList;
    }

    // TODO 2.4 软件版本维度日新数
    @RequestMapping("/user/appverDNU")
    @CrossOrigin
    List getAppverDNU(){
        List<DNUOneDim> appverDNUList = dnuDaoImpl.getOneDimDnuInfo("appver");
        return appverDNUList;
    }

    // TODO 2.5 设备类型维度日新数
    @RequestMapping("/user/devicetypeDNU")
    @CrossOrigin
    List getDevicetypeDNUDNU(){
        List<DNUOneDim> devicetypeDNUList = dnuDaoImpl.getOneDimDnuInfo("devicetype");
        return devicetypeDNUList;
    }


    // TODO 3. 流量概况分析
    // TODO 3.1 省份维度流量分析
    @RequestMapping("/traffic/province")
    List getProvinceTraffic(){
        List<Traffic> provinceTraffic = trafficImpl.getDimTrafficInfo("province");
        return provinceTraffic;
    }

    // TODO 3.2 城市维度流量分析
    @RequestMapping("/traffic/city")
    @CrossOrigin
    List getCityTraffic(){
        List<Traffic> cityTraffic = trafficImpl.getDimTrafficInfo("city");
        return cityTraffic;
    }

    // TODO 3.3 区县维度流量分析
    @RequestMapping("/traffic/district")
    @CrossOrigin
    List getDistrictTraffic(){
        List<Traffic> districtTraffic = trafficImpl.getDimTrafficInfo("district");
        return districtTraffic;
    }

    // TODO 3.4 设备类型维度流量分析
    @RequestMapping("/traffic/devicetype")
    @CrossOrigin
    List getDevicetypeTraffic(){
        List<Traffic> devicetypeTraffic = trafficImpl.getDimTrafficInfo("devicetype");
        return devicetypeTraffic;
    }

    // TODO 3.5 操作系统名称维度流量分析
    @RequestMapping("/traffic/osname")
    @CrossOrigin
    List getOsnameTraffic(){
        List<Traffic> osnameTraffic = trafficImpl.getDimTrafficInfo("osname");
        return osnameTraffic;
    }

    // TODO 3.6 操作系统版本维度流量分析
    @RequestMapping("/traffic/osver")
    @CrossOrigin
    List getOsverTraffic(){
        List<Traffic> osverTraffic = trafficImpl.getDimTrafficInfo("osver");
        return osverTraffic;
    }

    // TODO 3.7 发行渠道维度流量分析
    @RequestMapping("/traffic/release_ch")
    @CrossOrigin
    List getRelease_chTraffic(){
        List<Traffic> release_chTraffic = trafficImpl.getDimTrafficInfo("release_ch");
        return release_chTraffic;
    }

    // TODO 3.8 升级渠道维度流量分析
    @RequestMapping("/traffic/promotion_ch")
    @CrossOrigin
    List getPromotion_chTraffic(){
        List promotion_chTraffic = trafficImpl.getDimTrafficInfo("promotion_ch");
        return promotion_chTraffic;
    }

    // TODO 4. 交互事件分析
    // TODO 4.1 交互事件概况分析
    @RequestMapping("/event/cnts_users")
    List getCntsUsersEvent(){
        List<Event> EventList = this.eventImpl.getEventInfo();
        return EventList;
    }

    // TODO 4.2 交互事件用户Top10分析
    // 1. accusationEvent 投诉事件用户top10
    @RequestMapping("/event/accusationEvent")
    List getAccusationEvent(){
        List<Eventid> accusationEventList = this.eventImpl.getEventIdInfo("accusationEvent");
        return accusationEventList;
    }

    // 2. appClickEvent 点击事件用户Top10
    @RequestMapping("/event/appClickEvent")
    List getAppClickEvent(){
        List<Eventid> appClickEventList = this.eventImpl.getEventIdInfo("appClickEvent");
        return appClickEventList;
    }

    // 3. appStartEvent 打开事件用户top10
    @RequestMapping("/event/appStartEvent")
    List getAppStartEvent(){
        List<Eventid> appStartEventList = this.eventImpl.getEventIdInfo("appStartEvent");
        return appStartEventList;
    }

    // 4. appviewEvent 浏览事件用户top10
    @RequestMapping("/event/appviewEvent")
    List getAppViewEvent(){
        List<Eventid> appStartEventList = this.eventImpl.getEventIdInfo("appviewEvent");
        return appStartEventList;
    }

    // 5. clickChannelEvent 点击频道事件用户top10
    @RequestMapping("/event/clickChannelEvent")
    List getClickChannelEvent(){
        List<Eventid> clickChannelEventList = this.eventImpl.getEventIdInfo("clickChannelEvent");
        return clickChannelEventList;
    }

    // 6. commentEvent 评论事件用户top10
    @RequestMapping("/event/commentEvent")
    List getCommentEvent(){
        List<Eventid> commentEventList = this.eventImpl.getEventIdInfo("commentEvent");
        return commentEventList;
    }

    // 7. enterTabEvent 进入选项事件用户top10
    @RequestMapping("/event/enterTabEvent")
    List getEnterTabEvent(){
        List<Eventid> enterTabEventList = this.eventImpl.getEventIdInfo("enterTabEvent");
        return enterTabEventList;
    }

    // 8. favoriteEvent 点赞事件用户top10
    @RequestMapping("/event/favoriteEvent")
    List getFavoriteEvent(){
        List<Eventid> favoriteEventList = this.eventImpl.getEventIdInfo("favoriteEvent");
        return favoriteEventList;
    }

    // 9. getCodeEvent 获取代码事件用户top10
    @RequestMapping("/event/getCodeEvent")
    List getGetCodeEvent(){
        List<Eventid> getCodeEventList = this.eventImpl.getEventIdInfo("getCodeEvent");
        return getCodeEventList;
    }

    // 10. installEvent 下载事件用户top10
    @RequestMapping("/event/installEvent")
    List getInstallEvent(){
        List<Eventid> installEventList = this.eventImpl.getEventIdInfo("installEvent");
        return installEventList;
    }

    // 11. loginEvent 登录事件用户top10
    @RequestMapping("/event/loginEvent")
    List getLoginEventEvent(){
        List<Eventid> loginEventList = this.eventImpl.getEventIdInfo("loginEvent");
        return loginEventList;
    }

    // 12. searchEvent 搜索事件用户top10
    @RequestMapping("/event/searchEvent")
    List getSearchEvent(){
        List<Eventid> searchEventList = this.eventImpl.getEventIdInfo("searchEvent");
        return searchEventList;
    }

    // 13. shareEvent 分享转发事件用户top10
    @RequestMapping("/event/shareEvent")
    List getShareEvent(){
        List<Eventid> shareEventList = this.eventImpl.getEventIdInfo("shareEvent");
        return shareEventList;
    }

    // 14. sinUpEvent 报名事件用户top10
    @RequestMapping("/event/sinUpEvent")
    List getSinUpEvent(){
        List<Eventid> sinUpEventList = this.eventImpl.getEventIdInfo("sinUpEvent");
        return sinUpEventList;
    }

    // 15. viewConentDetailEvent 查看内容详细信息事件用户top10
    @RequestMapping("/event/viewConentDetailEvent")
    List getViewConentDetailEvent(){
        List<Eventid> viewConentDetailEventList = this.eventImpl.getEventIdInfo("viewConentDetailEvent");
        return viewConentDetailEventList;
    }

    // 16. webStayEvent 网页停留事件用户top10
    @RequestMapping("/event/webStayEvent")
    List getWebStayEvent(){
        List<Eventid> webStayEventList = this.eventImpl.getEventIdInfo("webStayEvent");
        return webStayEventList;
    }

    // TODO 5. 广告事件分析
    @RequestMapping("/event/advertise")
    List getAdvertiseEvent(){
        List<Advertise> advertiseEventList = this.advertiseImpl.getAdvertiseInfo();
        return advertiseEventList;
    }

}
