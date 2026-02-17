
//--------------------------------------------------------------------------------
//source.cashseed
use source;
db.cashseed_trade_log.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.cashseed_trade_log.createIndex( {time: 1} );
db.cashseed_trade_log.createIndex( {time: 1});
db.cashseed_trade_log.createIndex( {act_type:1});

use admin;
db.runCommand( { enablesharding :"source"});
//db.runCommand( { shardcollection : "source.cashseed",key : {time: 'hashed'} } );
sh.shardCollection( "source.cashseed_trade_log", { time:1 } );


//---------------------------------------------------------------------------------
//用户每次开宝箱操作
//source.mc_bh_openbox
/* 
 data:
 {
 	"_id": ObjectId("5b504b7cfd4f0f508547dcc6"), 
 	"seed_num": 10, //发放种子数量
 	"open_times": 1, //每天第几次开启宝箱
 	"ISOdate": ISODate("2018-07-19T08:27:40.114Z"),
 	"date": "2018-07-19",
 	"time": NumberLong("1531988860114"),
 	"accum": 7, //连续几天开宝箱
 	"tvmid": "123111"
 }*/
use source;
db.mc_bh_openbox.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_bh_openbox.createIndex({time:1});
db.mc_bh_openbox.createIndex({accum:1});

//---------------------------------------------------------------------------------
//user_behavior.mc_page_load
use user_behavior;
db.mc_page_load.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_page_load.createIndex({date:1});
db.mc_page_load.createIndex({tvmid:'hashed'});
sh.shardCollection( "user_behavior.mc_page_load", {date:1});

db.mc_share_page_leave.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_share_page_leave.createIndex({date:1});
sh.shardCollection( "user_behavior.mc_share_page_leave", {date:1});

db.mc_share_app.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_share_app.createIndex({date:1});
sh.shardCollection( "user_behavior.mc_share_app", {date:1});

db.mc_page_leave.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_page_leave.createIndex({date:1});
sh.shardCollection( "user_behavior.mc_page_leave", {date:1});

db.mc_share.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_share.createIndex({date:1});
sh.shardCollection( "user_behavior.mc_share", {date:1});

db.mc_click_boxshare.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_click_boxshare.createIndex({date:1});


db.mc_page_load_zixun1.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_page_load_zixun1.createIndex({date:1});
db.mc_page_load_zixun1.createIndex({d:'hashed'});
sh.shardCollection( "user_behavior.mc_page_load_zixun1", {date:1});

db.mc_page_zixun_detail.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_page_zixun_detail.createIndex({time:1,event:1,cn:1});
db.mc_page_zixun_detail.createIndex({date:1});
db.mc_page_zixun_detail.createIndex({d:'hashed'});
db.mc_page_zixun_detail.createIndex({date:1,event:1,cn:1}, {background:1})
sh.shardCollection( "user_behavior.mc_page_zixun_detail", {date:1});

//-------------------------------------------------------------------
//看视频广告行为 2018-09-04
use user_behavior;
//用户点击动作，
db.mc_event_click.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_event_click.createIndex({date:1});
db.mc_event_click.createIndex({status:1,type:1});

db.mc_ad_video_show.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_ad_video_show.createIndex({date:1,cd:1});

//icon的用户点击行为
db.mc_event_icon.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_event_icon.createIndex({date:1});

//用户行为（偏功能性，如收藏，关注，取消收藏等）
db.mc_event_behavior.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.mc_event_behavior.createIndex({date:1});

//mc_page_load_zhuli

use user_behavior;

//广告资源位统计
db.xz_ad_pos.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_ad_pos.createIndex({date:1});
//app各种事件上报统计
db.xz_app_event.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_app_event.createIndex({date:1});
//签到统计
db.xz_checkin.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_checkin.createIndex({date:1});
//消息统计
db.xz_app_notify.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_app_notify.createIndex({date:1});
//刷红包统计
db.xz_redbag.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_redbag.createIndex({date:1});
//注册人数统计
db.xz_register.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_register.createIndex({date:1});
//分享统计
// db.xz_share.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
db.xz_share.createIndex({date:1});
db.xz_common_pvuv.createIndex({date:1}, {background:1})
db.xz_common_pvuv.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600}, {background:1})

// 小脉生活
db.pulse_material_event.createIndex({date:1}, {background:1})
db.pulse_material_event.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600}, {background:1})



use prop_card;
//道具卡统计
// db.xz_register.createIndex({ISOdate:1}, {expireAfterSeconds: 24*30*3600});
for (i=0;i<100;i++){
	collection = 'user_prop_'+i
	print(collection)
	db[collection].createIndex({draw_time:1});
	db[collection].createIndex({last_use_time:1,available_times:1});
	db[collection].createIndex({card_type:1});
	db[collection].createIndex({start_time:1});
	db[collection].createIndex({expire_time:1});
	db[collection].createIndex({card_id:1});
}
// db.xz_register.createIndex({date:1});