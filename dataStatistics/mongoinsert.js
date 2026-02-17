function getNowFormatDate() {
        var date = new Date();
        var seperator1 = "-";
        var year = date.getFullYear();
        var month = date.getMonth() + 1;
        var strDate = date.getDate();
        if (month >= 1 && month <= 9) {
            month = "0" + month;
        }
        if (strDate >= 0 && strDate <= 9) {
            strDate = "0" + strDate;
        }
        var currentdate = year + seperator1 + month + seperator1 + strDate;
        return currentdate;
}

//user_behaviour.mc_page_load_zixun
for (i=0;i<10000;i++){
  var randomNum = Math.ceil(Math.random() * 100)
  var randomNum1 = Math.ceil(Math.random() * 10)
  var timestamp = (new Date()).getTime();
  var myDate = new Date();
  var dates = getNowFormatDate();
  var data = {
      'd':'tvmid-test-'+randomNum,   //tvmid
      'pst':'news_bd',          //来源
      'cn':"app"         ,          //渠道, 默认app 
      'ptab': '推荐',
      'tags':[],  //文章标签
      'cats': '分类-'+randomNum1,                //分类
      'catid': randomNum1,                //分类id
      'ct': 'news',            //文章类型， 是视频还是图文  
      'position':{'x':11,'y':131.1},    //经纬度，如果没有，则留空
      'device': {}  ,                  
      'ip': '127.0.0.1',
      'time': timestamp,
      'page': 'list',
      'date': dates 
  };
  db.mc_page_load_zixun.insert(data)
  print (i)
}

//user_behaviour.mc_event_click
for (i=0;i<10000;i++){
  var user_rand = Math.ceil(Math.random() * 1000)
  var time_rand = Math.ceil(Math.random() * 1000)
  var timestamp = (new Date()).getTime();
  var dates = getNowFormatDate();
  var data = {
        'd':'tvmid-test-'+user_rand,   //tvmid
        'type':'ad_video',          //来源
        'status':0,          //渠道, 默认app 
        'ip': '127.0.0.1',
        'time': timestamp+time_rand*1000,
        'date': dates
        //'time': timestamp+(time_rand-24*3600)*1000,
        //'date': '2018-09-03'
    };
    db.mc_event_click.insert(data)
    print (i)
}

//user_behaviour.mc_event_icon
for (i=0;i<10000;i++){
  var user_rand = Math.ceil(Math.random() * 1000)
  var icon_type_rand = Math.round(Math.random())
  var iconType = ['app_icon', 'news']

  var icon_id_rand = Math.ceil(Math.random() * 20)
  var timestamp = (new Date()).getTime();
  var dates = getNowFormatDate();
  var data = {
    type: iconType[icon_type_rand], //app_icon:个人中心图标；news：资讯页浮标
    d: 'tvmid-'+user_rand,
    iid: icon_id_rand, //iconid 
    ititle: 'icon title '+icon_id_rand,
    event: 'click',
    ip: '127.0.0.1',
    time: timestamp+time_rand*1000,
    date: dates
  }
  db.mc_event_icon.insert(data)
}

//user_behaviour.mc_event_behavior
for (i=0;i<10000;i++){
  var user_rand = Math.ceil(Math.random() * 1000)
  var event_type_rand = Math.round(Math.random())
  var eventType = ['collect', 'cancel_collect']

  var news_type_rand = Math.round(Math.random())
  var newsType = ['news_tv', 'news_bd']

  var cat_id_rand = Math.ceil(Math.random() * 200)
  var aid = Math.ceil(Math.random() * 100)

  var ct_rand = Math.round(Math.random()*2)
  var ctType = ['video','news', 'image']
  var dates = getNowFormatDate();
  var data = {
      event: eventType[event_type_rand], //收藏｜取消收藏
      d: 'tvmid-'+user_rand, //收藏的行为人
      co: newsType[news_type_rand],
      aid: aid,
      atitle: 'article title ' + aid, //收藏对象名称
      catid: cat_id_rand, //收藏的对象所属分类
      cats: 'cat name '+cat_id_rand, //收藏的对象所属分类名称
      ct: ctType[ct_rand],
      ptab: '所属栏目',
      ptabid: 101,
      ip: '127.0.0.1',
      time: timestamp+time_rand*1000,
      date: dates
  }
  db.mc_event_behavior.insert(data)
}


//user_behaviour.mc_page_zixun_detail
for (i=0;i<100000;i++){
  var user_rand = Math.ceil(Math.random() * 1000)
  
  var pstType = ['news_bd', 'news_tv']
  var cnType = ['app','weixin','qq']
  var ctType = ['video','news','image']

  var pst_rand = Math.round(Math.random())
  var cn_rand = Math.round(Math.random()*2)
  var ct_rand = Math.round(Math.random()*2)

  var aid =  Math.ceil(Math.random() * 100)
  var cat_id =  Math.ceil(Math.random() * 30)
  var dates = getNowFormatDate();

  var time_rand = Math.ceil(Math.random() * 3600*12)
  var timestamp = (new Date()).getTime();

  timestamp = timestamp + time_rand *1000
  var data = {
      d: 'tvmid-'+user_rand,   //tvmid
      pst: pstType[pst_rand],            //来源
      cn: cnType[cn_rand],                    //渠道, 默认app 
      aid: aid,                       //文章id 
      atitle:'文章标题-'+aid,           //文章title
      tags: ['tag1','tag2','tag3'],  //文章标签
      cats: '分类名-'+cat_id,                 //分类名称
      catid: cat_id,        //分类ID
      ptab: 'tab名-'+cat_id,   //页面条目或栏目，可能与分类相同 
      ptabid: cat_id,
      ct: ctType[ct_rand],           //文章类型， 是视频还是图文  
      position:{x:'11',y:'131.1'},    //经纬度，如果没有，则留空
      device: {},
      ip: '127.0.0.1',
      time: timestamp,
      date: dates,
      page: 'detail',
      event: 'page_load'
  }

  print(i)
  db.mc_page_zixun_detail.insert(data)
}


      



