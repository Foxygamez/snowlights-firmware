// Snowboard LED v60 — ESP32 / MPU6050 / WS2811 / BLE
// Heel strip = GPIO18 (A), Toe strip = GPIO16 (B)
#include <Wire.h>
#include <MPU6050_light.h>
#include <FastLED.h>
#include <BLEDevice.h>
#include <BLEServer.h>
#include <BLEUtils.h>
#include <BLE2902.h>
#include <WiFi.h>
#include <WebServer.h>
#include <Update.h>

// ── OTA WiFi config ──────────────────────────────────────────
// Board hosts its own AP — connect phone to this network, then
// navigate to http://192.168.4.1 to upload new firmware.
#define OTA_SSID "SnowLights-Update"
#define OTA_PASS "snowboard"
WebServer otaSrv(80);
bool otaMode = false;
float otaCometPos = 0.0f;
unsigned long otaLastMs = 0;

#define FW_VERSION "60"
#define SVC_UUID "4fafc201-1fb5-459e-8fcc-c5c9c331914b"
#define CMD_UUID "beb5483e-36e1-4688-b7f5-ea07361b26a8"
#define STA_UUID "beb5483f-36e1-4688-b7f5-ea07361b26a8"
#define PIN_A 18
#define PIN_B 16
#define MAXN 144
#define MAXG 8

CRGB A[MAXN], B[MAXN];

// trigger

struct GS { uint8_t r,g,b,pos; }; // pos: 0-255 = position along strip
struct GD { GS s[MAXG]; uint8_t n; uint8_t blend; }; // blend: 0=RGB, 1=HSV wheel
enum Eff : uint8_t { FADE,PULSE,COMET,ZONES };

// ── Config (all runtime-settable via BLE) ────────────────────
struct Cfg {
  // Modes
  bool emg=0,carv=0,spd=0,stat=0;
  // Strip sizes
  uint8_t nA=20,nB=20;
  // Fade direction: 0=mid-out/ends-in, 1=fwd, 2=bwd; dirOut=out vs in
  bool dirOut=1; uint8_t dirFwd=0;
  // Effect
  Eff eff=FADE;
  uint8_t cometMode=0; // 0=fwd 1=rev 2=out 3=in
  // Static colours (per strip: 0=solid 1=grad 2=rainbow)
  uint8_t stMA=0,stMB=0;
  uint8_t stAr=255,stAg=255,stAb=255; GD stGdA={{{255,0,0,0},{0,0,255,255}},2,0};
  uint8_t stBr=255,stBg=255,stBb=255; GD stGdB={{{255,0,0,0},{0,0,255,255}},2,0};
  bool rbowRevStat=0;
  // Carving colours
  bool gA=0,carvRbowA=0; uint8_t sAr=255,sAg=255,sAb=255; GD gdA={{{255,0,0,0},{0,0,255,255}},2,0};
  bool gB=0,carvRbowB=0; uint8_t sBr=255,sBg=255,sBb=255; GD gdB={{{255,0,0,0},{0,0,255,255}},2,0};
  float dz=10,mx=45,sa=0.1f; int ss=10;
  uint8_t carvNeutral=128; // brightness at 0 degrees (no lean)
  uint8_t spdSrc=2;        // 0=IMU only, 1=GPS only, 2=GPS+IMU hybrid
  // Speed colours
  bool sgA=0,spdRbowA=0; uint8_t cAr=255,cAg=255,cAb=255; GD sgdA={{{255,255,255,0},{255,0,0,255}},2,0};
  bool sgB=0,spdRbowB=0; uint8_t cBr=255,cBg=255,cBb=255; GD sgdB={{{255,255,255,0},{255,0,0,255}},2,0};
  uint8_t ovr=255,ovg=128,ovb=0;
  float cap=25,fr=5;
  // Pulse
  uint8_t pmin=20,pmax=255,pspd=8;
  // Comet
  uint8_t ct=8,cspd=8;
  // Idle
  bool idleArmed=0,idleBreath=1,rbowRevIdle=0;
  uint16_t idleTo=10;
  float idleWakeG=0.3f;
  uint8_t ir=0,ig=80,ib=80,idleMode=0,ispd=4;
  GD idleGdA={{{0,80,80,0},{0,40,120,255}},2,0};
  // Emergency
  uint8_t dr=255,dg=0,db=0;
  bool dangGrad=0; GD dangGdA={{{255,0,0,0},{255,165,0,255}},2,0};
  uint32_t fm=500; float da=90;
  // Master brightness
  uint8_t masterBr=255;
  // Heatmap (reset by cmd)
  uint16_t hmLeft=0,hmRight=0;
  float hmMaxRoll=0;
} cfg;

// ── State ────────────────────────────────────────────────────
float rbuf[20]; int ri=0; bool rfull=0;
float smA=0,smB=0;
float gx=0,gy=0,gz=9.8f;
const float G=9.80665f,AD=0.08f;
const float GRAV_TAU=1.5f;   // gravity LPF time constant (seconds)
const float GYRO_GATE=200.0f;  // °/s — only block on violent jerks, not carving turns
const float IMU_SCALE=0.4f;    // IMU acceleration → mph contribution scale
const float IMU_DECAY=3.0f;    // IMU velocity decay — aggressive zero when still
float vmp=0;                       // displayed speed mph — GPS-primary, IMU-smoothed
float imuDelta=0;                  // accumulated IMU speed adjustment since last GPS
float pdm=0;                       // previous frame acceleration magnitude
float gpsVmp=-1.0f;               // last GPS speed mph, -1 = no GPS
float gpsAge=0;                    // seconds since last GPS update
unsigned long lt=0,lmt=0,stimer=0;
float pp=0,ip=0;          // pulse phase, idle phase
float cpA=0,cpB=0;        // comet head positions
float tailA[MAXN]={},tailB[MAXN]={};
float ima=0,idleFade=0;
bool idle=0;
bool hmInDeadzone=1;
#define IFADE 0.4f
volatile bool recal=0;
MPU6050 mpu(Wire);
BLEServer* srv=nullptr;
BLECharacteristic* sta=nullptr;
bool conn=0,oconn=0;

// ── Helpers ──────────────────────────────────────────────────
static inline CRGB sc(uint8_t r,uint8_t g,uint8_t b2,uint8_t br){
  return CRGB((uint16_t)r*br/255,(uint16_t)g*br/255,(uint16_t)b2*br/255);
}
static inline uint8_t lp(uint8_t a,uint8_t ib,float t){
  return (uint8_t)(a+(float)(ib-a)*t);
}
static inline CRGB hue(uint8_t h,uint8_t br){ return CHSV(h,255,br); }

// Oklab removed — not used in current build
static inline void rgb2hsv(uint8_t r,uint8_t g,uint8_t b,float&h,float&s,float&v){
  float rf=r/255.0f,gf=g/255.0f,bf=b/255.0f;
  float mx=fmaxf(rf,fmaxf(gf,bf)),mn=fminf(rf,fminf(gf,bf)),dd=mx-mn;
  v=mx; s=(mx<0.001f)?0:dd/mx;
  if(dd<0.001f){h=0;return;}
  if(mx==rf)      h=60.0f*fmodf((gf-bf)/dd,6.0f);
  else if(mx==gf) h=60.0f*((bf-rf)/dd+2.0f);
  else            h=60.0f*((rf-gf)/dd+4.0f);
  if(h<0)h+=360.0f;
}
static CRGB gcol(const GD& d,int i,int n){
  if(d.n<2) return CRGB(d.s[0].r,d.s[0].g,d.s[0].b);
  float t=(float)i/(n>1?n-1:1);
  // Find surrounding stops by position (0-255 mapped to 0.0-1.0)
  int k=0;
  for(int j=0;j<d.n-1;j++){
    if(t<=d.s[j+1].pos/255.0f){k=j;break;}
    k=j;
  }
  float p0=d.s[k].pos/255.0f, p1=d.s[k+1].pos/255.0f;
  float span=p1-p0;
  float f=(span<0.001f)?0.0f:constrain((t-p0)/span,0.0f,1.0f);
  if(d.blend>=1){
    float arc=d.blend/100.0f;
    float h0,s0,v0,h1,s1,v1;
    rgb2hsv(d.s[k].r,d.s[k].g,d.s[k].b,h0,s0,v0);
    rgb2hsv(d.s[k+1].r,d.s[k+1].g,d.s[k+1].b,h1,s1,v1);
    float dh=h1-h0;
    if(dh>180)dh-=360; if(dh<-180)dh+=360;
    float hh=fmodf(h0+dh*arc*f+360.0f,360.0f);
    float s=s0+(s1-s0)*f, v=v0+(v1-v0)*f;
    float c=v*s,x=c*(1.0f-fabsf(fmodf(hh/60.0f,2.0f)-1.0f)),m=v-c;
    float rf=0,gf=0,bf=0;
    int hi=(int)(hh/60)%6;
    if(hi==0){rf=c;gf=x;}else if(hi==1){rf=x;gf=c;}
    else if(hi==2){gf=c;bf=x;}else if(hi==3){gf=x;bf=c;}
    else if(hi==4){rf=x;bf=c;}else{rf=c;bf=x;}
    uint8_t hr=(uint8_t)((rf+m)*255),hg=(uint8_t)((gf+m)*255),hb=(uint8_t)((bf+m)*255);
    if(arc>=1.0f) return CRGB(hr,hg,hb);
    uint8_t rr=lp(d.s[k].r,d.s[k+1].r,f),rg=lp(d.s[k].g,d.s[k+1].g,f),rb=lp(d.s[k].b,d.s[k+1].b,f);
    return CRGB(lp(rr,hr,arc),lp(rg,hg,arc),lp(rb,hb,arc));
  }
  return CRGB(lp(d.s[k].r,d.s[k+1].r,f),lp(d.s[k].g,d.s[k+1].g,f),lp(d.s[k].b,d.s[k+1].b,f));
}
static void gradFull(CRGB* L,int n,const GD& d,uint8_t br){
  for(int i=0;i<n;i++){CRGB c=gcol(d,i,n);L[i]=sc(c.r,c.g,c.b,br);}
}
static inline CRGB spdCol(CRGB base,uint8_t or_,uint8_t og,uint8_t ob,float of,uint8_t br){
  return sc(lp(base.r,or_,of),lp(base.g,og,of),lp(base.b,ob,of),br);
}
static void rainbowStrip(CRGB* L,int n,uint8_t br,float off,int len){
  for(int i=0;i<n;i++){
    uint8_t h=(uint8_t)(fmodf(off+i,(float)len)/len*255.0f);
    L[i]=hue(h,br);
  }
}

// ── FADE ─────────────────────────────────────────────────────
static void doFade(CRGB* L,int n,bool ug,const GD& gd,
    uint8_t r,uint8_t g,uint8_t ib,uint8_t or_,uint8_t og,uint8_t ob,
    float ff,float of,uint8_t lb,bool out,uint8_t df=0){
  fill_solid(L,n,CRGB::Black);
  if(of>=1.0f){fill_solid(L,n,sc(or_,og,ob,lb));return;}
  if(ff<=0.0f) return;
  int lit=(int)(ff*n);
  if(lit<=0) return;
  if(df){
    // fwd (df=1) or bwd (df=2) linear fill
    for(int i=0;i<lit;i++){
      int idx=(df==1)?i:n-1-i;
      CRGB base=ug?gcol(gd,idx,n):CRGB(r,g,ib);
      L[idx]=spdCol(base,or_,og,ob,of,lb);
    }
    return;
  }
  int half=lit/2, centre=n/2;
  if(out){
    if(lit&1){CRGB base=ug?gcol(gd,centre,n):CRGB(r,g,ib);L[centre]=spdCol(base,or_,og,ob,of,lb);}
    for(int s=0;s<half;s++){
      int lo=centre-s, hi=centre+s+1;
      if(lo>=0){CRGB col=ug?gcol(gd,lo,n):CRGB(r,g,ib);L[lo]=spdCol(col,or_,og,ob,of,lb);}
      if(hi<n) {CRGB col=ug?gcol(gd,hi,n):CRGB(r,g,ib);L[hi]=spdCol(col,or_,og,ob,of,lb);}
    }
  } else {
    for(int s=0;s<half;s++){
      int lo=s,hi=n-1-s;
      CRGB bL=ug?gcol(gd,lo,n):CRGB(r,g,ib);
      CRGB bR=ug?gcol(gd,hi,n):CRGB(r,g,ib);
      L[lo]=spdCol(bL,or_,og,ob,of,lb);
      if(hi!=lo)L[hi]=spdCol(bR,or_,og,ob,of,lb);
    }
  }
}

// ── PULSE ────────────────────────────────────────────────────
static void doPulse(CRGB* L,int n,bool ug,const GD& gd,
    uint8_t r,uint8_t g,uint8_t ib,uint8_t or_,uint8_t og,uint8_t ob,
    float of,uint8_t lb,uint8_t pmin,uint8_t pmax){
  uint8_t bri=(uint8_t)(pmin+0.5f*(1.0f-cosf(pp))*(pmax-pmin));
  bri=(uint8_t)((uint16_t)bri*lb/255);
  bool over=(of>=1.0f);
  for(int i=0;i<n;i++){
    CRGB c=over?CRGB(or_,og,ob):(ug?gcol(gd,i,n):CRGB(r,g,ib));
    if(!over&&of>0)c=CRGB(lp(c.r,or_,of),lp(c.g,og,of),lp(c.b,ob,of));
    L[i]=sc(c.r,c.g,c.b,bri);
  }
}

// ── COMET ────────────────────────────────────────────────────
static void doComet(CRGB* L,int n,bool ug,const GD& gd,
    uint8_t r,uint8_t g,uint8_t ib,uint8_t or_,uint8_t og,uint8_t ob,
    float cp,float of,uint8_t lb,uint8_t tl,uint8_t mode,float* tail,float dt){
  int pos=(int)cp;
  if(mode<2){
    // fwd/rev: simple wrapping comet
    fill_solid(L,n,CRGB::Black);
    bool fwd=(mode==0);
    int head=fwd?pos:n-1-pos;
    bool over=(of>=1.0f);
    for(int i=0;i<n;i++){
      int dist=fwd?(head-i+n)%n:(i-head+n)%n;
      if(dist>tl) continue;
      float fade=1.0f-(float)dist/(tl+1);
      CRGB base=over?CRGB(or_,og,ob):(ug?gcol(gd,i,n):CRGB(r,g,ib));
      if(!over&&of>0)base=CRGB(lp(base.r,or_,of),lp(base.g,og,of),lp(base.b,ob,of));
      L[i]=sc(base.r,base.g,base.b,(uint8_t)(fade*lb));
    }
  } else {
    // out/in: soft-tail decay buffer
    int half=n/2, p=pos%half;
    float decay=1.0f-dt*12.0f; if(decay<0)decay=0;
    for(int i=0;i<n;i++) tail[i]*=decay;
    for(int i=0;i<n;i++){
      int dist;
      if(mode==2) dist=(i<half)?(i-(half-1-p)):(half+p-i);
      else        dist=(i<half)?(p-i):(i-(n-1-p));
      if(dist>=0&&dist<=tl){
        float fade=1.0f-(float)dist/(tl+1);
        if(fade>tail[i]) tail[i]=fade;
      }
    }
    fill_solid(L,n,CRGB::Black);
    bool over=(of>=1.0f);
    for(int i=0;i<n;i++){
      if(tail[i]<0.01f) continue;
      CRGB base=over?CRGB(or_,og,ob):(ug?gcol(gd,i,n):CRGB(r,g,ib));
      if(!over&&of>0)base=CRGB(lp(base.r,or_,of),lp(base.g,og,of),lp(base.b,ob,of));
      L[i]=sc(base.r,base.g,base.b,(uint8_t)(tail[i]*lb));
    }
  }
}

// ── IDLE ─────────────────────────────────────────────────────
static void renderIdle(){
  float s01=0.5f*(1.0f-cosf(ip));
  uint8_t br=cfg.idleBreath?(uint8_t)((10+s01*190)*idleFade):(uint8_t)(200*idleFade);
  if(cfg.idleMode==2){
    int len=cfg.nA+cfg.nB;
    float scroll=fmodf(ip/(2*PI)*len,(float)len);
    if(cfg.rbowRevIdle) scroll=fmodf(-scroll+len,(float)len);
    rainbowStrip(A,cfg.nA,br,scroll,len);
    // B runs reversed so strips form a continuous ring
    for(int i=0;i<cfg.nB;i++){
      uint8_t h=(uint8_t)(fmodf(scroll+cfg.nA+(cfg.nB-1-i),(float)len)/len*255.0f);
      B[i]=hue(h,br);
    }
  } else if(cfg.idleMode==1){
    gradFull(A,cfg.nA,cfg.idleGdA,br);
    gradFull(B,cfg.nB,cfg.idleGdA,br);
  } else {
    CRGB col=sc(cfg.ir,cfg.ig,cfg.ib,br);
    for(int i=0;i<cfg.nA;i++)A[i]=col;
    for(int i=0;i<cfg.nB;i++)B[i]=col;
  }
}

// ── Gradient parser ───────────────────────────────────────────
static void parseGrad(const char* s,GD& d){
  // Format: N,blend,pos,r,g,b,pos,r,g,b,...
  // blend: 0=RGB direct, 1-100=HSV wheel arc %
  // pos: 0-255 = position along strip
  int n=atoi(s); if(n<2)n=2; if(n>MAXG)n=MAXG; d.n=n;
  while(*s&&*s!=',')s++; if(*s)s++;
  d.blend=(uint8_t)atoi(s); while(*s&&*s!=',')s++; if(*s)s++;
  for(int i=0;i<n&&*s;i++){
    d.s[i].pos=(uint8_t)atoi(s);while(*s&&*s!=',')s++;if(*s)s++;
    d.s[i].r  =(uint8_t)atoi(s);while(*s&&*s!=',')s++;if(*s)s++;
    d.s[i].g  =(uint8_t)atoi(s);while(*s&&*s!=',')s++;if(*s)s++;
    d.s[i].b  =(uint8_t)atoi(s);while(*s&&*s!=',')s++;if(*s)s++;
  }
}

// ── BLE commands ─────────────────────────────────────────────
static void handleCmd(const char* c){
  #define IS(x)  (strcmp(c,x)==0)
  #define SW(x)  (strncmp(c,x,strlen(x))==0)
  #define AT(x)  (c+strlen(x))
  #define RGB(x) sscanf(AT(x),"%hhu,%hhu,%hhu"
  // System
  if(IS("RECALIBRATE")){recal=1;}
  else if(IS("OTA_MODE")){startOTA();}
  else if(SW("STRIP_LEN_A:")){cfg.nA=(uint8_t)constrain(atoi(AT("STRIP_LEN_A:")),5,MAXN);}
  else if(SW("STRIP_LEN_B:")){cfg.nB=(uint8_t)constrain(atoi(AT("STRIP_LEN_B:")),5,MAXN);}
  else if(SW("MASTER_BR:"))  {cfg.masterBr=(uint8_t)constrain(atoi(AT("MASTER_BR:")),0,255);}
  // Modes
  else if(SW("EMERGENCY:")){cfg.emg=c[10]=='1';}
  else if(SW("CARVING:"))  {cfg.carv=c[8]=='1';}
  else if(SW("SPEED:"))    {cfg.spd=c[6]=='1'; if(!cfg.spd){vmp=0.0f;imuDelta=0.0f;}}
  else if(SW("GPS_SPEED:")){
    gpsVmp=atof(AT("GPS_SPEED:"));
    gpsAge=0.0f;
    if(cfg.spdSrc==1||cfg.spdSrc==2){
      // Take GPS speed directly — no blending, no combining with IMU.
      // User wants GPS to immediately set the speed. Standstill = 0 right away.
      vmp=gpsVmp;
      imuDelta=0.0f; // reset IMU so it doesn't add to GPS-corrected baseline
    }
  }
  else if(SW("SPD_SRC:"))   {cfg.spdSrc=(uint8_t)constrain(atoi(AT("SPD_SRC:")),0,2);imuDelta=0.0f;vmp=0.0f;}
  else if(SW("STATIC:"))   {cfg.stat=c[7]=='1';}
  // Speed effect
  else if(IS("SPD_DIR:out")){cfg.dirOut=1;cfg.dirFwd=0;}
  else if(IS("SPD_DIR:in")) {cfg.dirOut=0;cfg.dirFwd=0;}
  else if(IS("SPD_DIR:fwd")){cfg.dirFwd=1;}
  else if(IS("SPD_DIR:bwd")){cfg.dirFwd=2;}
  else if(SW("SPD_MODE:")){const char*v=AT("SPD_MODE:");
    cfg.eff=!strcmp(v,"pulse")?PULSE:!strcmp(v,"comet")?COMET:!strcmp(v,"zones")?ZONES:FADE;
    cfg.spd=true;} // activating any speed mode enables speed rendering
  else if(SW("SPD_CAP:"))       {cfg.cap=constrain(atof(AT("SPD_CAP:")),1,50);}
  else if(SW("SPD_FADE_RANGE:")){cfg.fr=atof(AT("SPD_FADE_RANGE:"));}
  else if(SW("PULSE_MIN_BR:"))  {cfg.pmin=(uint8_t)constrain(atoi(AT("PULSE_MIN_BR:")),0,255);}
  else if(SW("PULSE_MAX_BR:"))  {cfg.pmax=(uint8_t)constrain(atoi(AT("PULSE_MAX_BR:")),0,255);}
  else if(SW("PULSE_SPEED:"))   {cfg.pspd=(uint8_t)constrain(atoi(AT("PULSE_SPEED:")),1,20);}
  else if(SW("COMET_LEN:"))     {cfg.ct=(uint8_t)constrain(atoi(AT("COMET_LEN:")),1,MAXN-1);}
  else if(SW("COMET_SPEED:"))   {cfg.cspd=(uint8_t)constrain(atoi(AT("COMET_SPEED:")),1,20);}
  else if(IS("COMET_DIR:fwd")){cfg.cometMode=0;}
  else if(IS("COMET_DIR:rev")){cfg.cometMode=1;}
  else if(IS("COMET_DIR:out")){cfg.cometMode=2;}
  else if(IS("COMET_DIR:in")) {cfg.cometMode=3;}
  // Static colours
  else if(IS("STATIC_MODE_A:solid"))   {cfg.stMA=0;}
  else if(IS("STATIC_MODE_A:gradient")){cfg.stMA=1;}
  else if(IS("STATIC_MODE_A:rainbow")) {cfg.stMA=2;}
  else if(IS("STATIC_MODE_B:solid"))   {cfg.stMB=0;}
  else if(IS("STATIC_MODE_B:gradient")){cfg.stMB=1;}
  else if(IS("STATIC_MODE_B:rainbow")) {cfg.stMB=2;}
  else if(IS("STAT_RBOW_REV:1")){cfg.rbowRevStat=1;}
  else if(IS("STAT_RBOW_REV:0")){cfg.rbowRevStat=0;}
  else if(SW("STATIC_COLOR_A:"))    {RGB("STATIC_COLOR_A:"),&cfg.stAr,&cfg.stAg,&cfg.stAb);}
  else if(SW("STATIC_COLOR_B:"))    {RGB("STATIC_COLOR_B:"),&cfg.stBr,&cfg.stBg,&cfg.stBb);}
  else if(SW("STATIC_GRAD_A_STOPS:")){parseGrad(AT("STATIC_GRAD_A_STOPS:"),cfg.stGdA);}
  else if(SW("STATIC_GRAD_B_STOPS:")){parseGrad(AT("STATIC_GRAD_B_STOPS:"),cfg.stGdB);}
  // Carving colours
  else if(IS("COLOR_MODE_A:solid"))   {cfg.gA=0;}
  else if(IS("COLOR_MODE_A:gradient")){cfg.gA=1;}
  else if(IS("COLOR_MODE_B:solid"))   {cfg.gB=0;}
  else if(IS("COLOR_MODE_B:gradient")){cfg.gB=1;}
  else if(IS("CARV_RBOW_A:1")){cfg.carvRbowA=1;}
  else if(IS("CARV_RBOW_A:0")){cfg.carvRbowA=0;}
  else if(IS("CARV_RBOW_B:1")){cfg.carvRbowB=1;}
  else if(IS("CARV_RBOW_B:0")){cfg.carvRbowB=0;}
  else if(SW("SOLID_A:"))     {RGB("SOLID_A:"),&cfg.sAr,&cfg.sAg,&cfg.sAb);}
  else if(SW("SOLID_B:"))     {RGB("SOLID_B:"),&cfg.sBr,&cfg.sBg,&cfg.sBb);}
  else if(SW("GRAD_A_STOPS:")){parseGrad(AT("GRAD_A_STOPS:"),cfg.gdA);}
  else if(SW("GRAD_B_STOPS:")){parseGrad(AT("GRAD_B_STOPS:"),cfg.gdB);}
  else if(SW("DEADZONE:"))    {cfg.dz=atof(AT("DEADZONE:"));}
  else if(SW("MAX_ANGLE:"))   {cfg.mx=atof(AT("MAX_ANGLE:"));}
  else if(SW("SMOOTH_SAMPLES:")){cfg.ss=constrain(atoi(AT("SMOOTH_SAMPLES:")),1,20);
    memset(rbuf,0,sizeof(rbuf));ri=0;rfull=0;}
  else if(SW("SMOOTH_ALPHA:")){cfg.sa=constrain(atof(AT("SMOOTH_ALPHA:")),0.01f,1.0f);}
  else if(SW("CARV_NEUTRAL:")){cfg.carvNeutral=(uint8_t)constrain(atoi(AT("CARV_NEUTRAL:")),0,255);}
  // Speed colours
  else if(IS("SPD_MODE_A:solid"))   {cfg.sgA=0;}
  else if(IS("SPD_MODE_A:gradient")){cfg.sgA=1;}
  else if(IS("SPD_MODE_B:solid"))   {cfg.sgB=0;}
  else if(IS("SPD_MODE_B:gradient")){cfg.sgB=1;}
  else if(IS("SPD_RBOW_A:1")){cfg.spdRbowA=1;}
  else if(IS("SPD_RBOW_A:0")){cfg.spdRbowA=0;}
  else if(IS("SPD_RBOW_B:1")){cfg.spdRbowB=1;}
  else if(IS("SPD_RBOW_B:0")){cfg.spdRbowB=0;}
  else if(SW("SPD_COLOR_A:"))    {RGB("SPD_COLOR_A:"),&cfg.cAr,&cfg.cAg,&cfg.cAb);}
  else if(SW("SPD_COLOR_B:"))    {RGB("SPD_COLOR_B:"),&cfg.cBr,&cfg.cBg,&cfg.cBb);}
  else if(SW("ZONE_COLOR:"))     {RGB("ZONE_COLOR:"),&cfg.cAr,&cfg.cAg,&cfg.cAb);
    cfg.cBr=cfg.cAr;cfg.cBg=cfg.cAg;cfg.cBb=cfg.cAb;}  // sets both strips
  else if(SW("SPD_GRAD_A_STOPS:")){parseGrad(AT("SPD_GRAD_A_STOPS:"),cfg.sgdA);}
  else if(SW("SPD_GRAD_B_STOPS:")){parseGrad(AT("SPD_GRAD_B_STOPS:"),cfg.sgdB);}
  else if(SW("SPD_OVER_COLOR:")){RGB("SPD_OVER_COLOR:"),&cfg.ovr,&cfg.ovg,&cfg.ovb);}
  // Idle
  else if(SW("IDLE_ARMED:")){cfg.idleArmed=c[11]=='1';
    if(!cfg.idleArmed){idle=0;idleFade=0;}lmt=millis();}
  else if(SW("IDLE_TIMEOUT_S:")){cfg.idleTo=(uint16_t)atoi(AT("IDLE_TIMEOUT_S:"));}
  else if(SW("IDLE_WAKE_G:")) {cfg.idleWakeG=constrain(atof(AT("IDLE_WAKE_G:")),0.05f,3.0f);}
  else if(SW("IDLE_COLOR:"))  {RGB("IDLE_COLOR:"),&cfg.ir,&cfg.ig,&cfg.ib);}
  else if(SW("IDLE_GRAD_STOPS:")){parseGrad(AT("IDLE_GRAD_STOPS:"),cfg.idleGdA);}
  else if(SW("IDLE_MODE:")){const char*v=AT("IDLE_MODE:");
    cfg.idleMode=!strcmp(v,"rainbow")?2:!strcmp(v,"gradient")?1:0;}
  else if(SW("IDLE_SPEED:"))   {cfg.ispd=(uint8_t)constrain(atoi(AT("IDLE_SPEED:")),1,20);}
  else if(IS("IDLE_BREATH:1")) {cfg.idleBreath=1;}
  else if(IS("IDLE_BREATH:0")) {cfg.idleBreath=0;}
  else if(IS("IDLE_RBOW_REV:1")){cfg.rbowRevIdle=1;}
  else if(IS("IDLE_RBOW_REV:0")){cfg.rbowRevIdle=0;}
  // Emergency
  else if(SW("DANGER_COLOR:"))     {RGB("DANGER_COLOR:"),&cfg.dr,&cfg.dg,&cfg.db);}
  else if(SW("DANGER_GRAD:"))      {cfg.dangGrad=c[12]=='1';}
  else if(SW("DANGER_GRAD_STOPS:")){parseGrad(AT("DANGER_GRAD_STOPS:"),cfg.dangGdA);}
  else if(SW("FLASH_MS:"))         {cfg.fm=(uint32_t)atoi(AT("FLASH_MS:"));}
  else if(SW("DANGER_ANGLE:"))     {cfg.da=atof(AT("DANGER_ANGLE:"));}
  // Heatmap
  else if(IS("HM_RESET")){cfg.hmLeft=0;cfg.hmRight=0;cfg.hmMaxRoll=0;}
  #undef IS
  #undef SW
  #undef AT
  #undef RGB
}

// ── Connect intro ─────────────────────────────────────────────
static void playIntro(){
  for(int f=0;f<60;f++){
    float t=(float)f/60, front=t*1.8f, bright=sinf(t*PI);
    for(int i=0;i<cfg.nA;i++){
      float w=bright*max(0.0f,1.0f-fabsf((float)abs(i-cfg.nA/2)/(cfg.nA/2)-front)*3.5f);
      A[i]=CRGB((uint8_t)(w*255),(uint8_t)(w*220),(uint8_t)(w*160));
    }
    for(int i=0;i<cfg.nB;i++){
      float w=bright*max(0.0f,1.0f-fabsf((float)abs(i-cfg.nB/2)/(cfg.nB/2)-front)*3.5f);
      B[i]=CRGB((uint8_t)(w*255),(uint8_t)(w*220),(uint8_t)(w*160));
    }
    FastLED.show(); delay(13);
  }
  fill_solid(A,cfg.nA,CRGB::Black);
  fill_solid(B,cfg.nB,CRGB::Black);
  FastLED.show();
}

class SrvCB:public BLEServerCallbacks{
  void onConnect(BLEServer*)   override{conn=1;}
  void onDisconnect(BLEServer*)override{conn=0;}
};
class CmdCB:public BLECharacteristicCallbacks{
  void onWrite(BLECharacteristic* p)override{
    std::string v=p->getValue(); while(!v.empty()&&(v.back()=='\r'||v.back()=='\n'||v.back()==' '))v.pop_back(); handleCmd(v.c_str());
  }
};


// ── OTA Handlers ──────────────────────────────────────────────
void otaHandleRoot(){
  otaSrv.send(200,"text/html",
    "<!DOCTYPE html><html><head><meta name='viewport' content='width=device-width'>"
    "<title>SnowLights OTA</title>"
    "<style>body{background:#0a0a0a;color:#4af0a0;font-family:monospace;display:flex;"
    "flex-direction:column;align-items:center;justify-content:center;height:100vh;margin:0;gap:16px}"
    "h2{letter-spacing:.2em;margin:0}label{font-size:12px;color:#888;letter-spacing:.1em}"
    "input[type=file]{color:#4af0a0;border:1px solid #4af0a0;padding:8px;border-radius:8px;width:280px}"
    "button{background:rgba(74,240,160,.1);border:1.5px solid #4af0a0;color:#4af0a0;"
    "padding:12px 32px;border-radius:10px;font-family:monospace;font-size:14px;"
    "letter-spacing:.15em;cursor:pointer;width:280px}"
    "progress{width:280px;height:8px;border-radius:4px}"
    "#msg{font-size:11px;color:#888;letter-spacing:.05em}</style></head>"
    "<body><h2>SNOWLIGHTS OTA</h2>"
    "<label>SELECT FIRMWARE (.bin)</label>"
    "<form method='POST' action='/update' enctype='multipart/form-data' id='f'>"
    "<input type='file' name='firmware' accept='.bin' required>"
    "<progress id='prog' value='0' max='100' style='display:none'></progress>"
    "<button type='submit'>▶ FLASH FIRMWARE</button>"
    "<div id='msg'>Board will restart after update.</div>"
    "</form>"
    "<script>document.getElementById('f').addEventListener('submit',function(e){"
    "e.preventDefault();const fd=new FormData(this);"
    "const xhr=new XMLHttpRequest();xhr.open('POST','/update');"
    "xhr.upload.onprogress=function(ev){if(ev.lengthComputable){"
    "const p=document.getElementById('prog');"
    "p.style.display='';p.value=Math.round(ev.loaded/ev.total*100);}}"
    "xhr.onload=function(){document.getElementById('msg').textContent="
    "xhr.status===200?'✓ Done! Restarting...':'✗ Error: '+xhr.responseText;}"
    "xhr.send(fd);});</script></body></html>"
  );
}
void otaHandleUpdate(){
  otaSrv.sendHeader("Connection","close");
  otaSrv.send(200,"text/plain",
    Update.hasError()?"FAIL":"OK");
  ESP.restart();
}
void otaHandleUpload(){
  HTTPUpload& up=otaSrv.upload();
  if(up.status==UPLOAD_FILE_START){
    Update.begin(UPDATE_SIZE_UNKNOWN);
  } else if(up.status==UPLOAD_FILE_WRITE){
    Update.write(up.buf,up.currentSize);
  } else if(up.status==UPLOAD_FILE_END){
    Update.end(true);
  }
}
void startOTA(){
  // Stop BLE so WiFi has full radio
  BLEDevice::deinit(true);
  delay(100);
  WiFi.softAP(OTA_SSID, OTA_PASS);
  otaSrv.on("/",HTTP_GET,otaHandleRoot);
  otaSrv.on("/update",HTTP_POST,otaHandleUpdate,otaHandleUpload);
  otaSrv.begin();
  otaMode=true;
  // OTA mode indicator — will animate in loop
  otaCometPos=0.0f;
  fill_solid(A,MAXN,CRGB::Black);
  fill_solid(B,MAXN,CRGB::Black);
  FastLED.show();
}

void setup(){
  memset(rbuf,0,sizeof(rbuf));
  FastLED.addLeds<WS2811,PIN_A,BRG>(A,MAXN);
  FastLED.addLeds<WS2811,PIN_B,BRG>(B,MAXN);
  FastLED.setBrightness(255);
  Wire.begin(); mpu.begin();
  delay(1000); mpu.calcOffsets(); mpu.update();
  gx=mpu.getAccX()*G; gy=mpu.getAccY()*G; gz=mpu.getAccZ()*G;
  lt=lmt=millis();
  BLEDevice::init("SnowboardLights");
  srv=BLEDevice::createServer(); srv->setCallbacks(new SrvCB());
  auto* svc=srv->createService(SVC_UUID);
  auto* cmd=svc->createCharacteristic(CMD_UUID,
    BLECharacteristic::PROPERTY_WRITE|BLECharacteristic::PROPERTY_WRITE_NR);
  cmd->setCallbacks(new CmdCB());
  sta=svc->createCharacteristic(STA_UUID,BLECharacteristic::PROPERTY_NOTIFY);
  sta->addDescriptor(new BLE2902());
  svc->start();
  auto* adv=BLEDevice::getAdvertising();
  adv->addServiceUUID(SVC_UUID); adv->setScanResponse(true); adv->setMinPreferred(0x06);
  BLEDevice::startAdvertising();
}

void loop(){
  if(otaMode){
    otaSrv.handleClient();
    // Animate circling white comet across both strips during OTA
    unsigned long now=millis();
    if(now-otaLastMs>=16){ // ~60fps
      otaLastMs=now;
      int totalLen=MAXN+MAXN;
      int tailLen=12;
      fill_solid(A,MAXN,CRGB::Black);
      fill_solid(B,MAXN,CRGB::Black);
      // Draw comet tail
      for(int t=0;t<tailLen;t++){
        int pos=((int)otaCometPos-t+totalLen*2)%totalLen;
        uint8_t br=(uint8_t)(255*(tailLen-t)/tailLen);
        CRGB col=CRGB(br,br,br); // white
        if(pos<MAXN) A[pos]=col;
        else B[pos-MAXN]=col;
      }
      FastLED.show();
      otaCometPos+=0.8f;
      if(otaCometPos>=totalLen) otaCometPos=0;
    }
    return;
  }
  if(!conn&&oconn){delay(400);srv->startAdvertising();oconn=0;}
  if(conn&&!oconn){oconn=1;playIntro();lt=millis();}

  if(recal){
    recal=0;
    for(int i=0;i<MAXN;i++){A[i]=B[i]=CRGB::Black;} FastLED.show();
    delay(500); mpu.calcOffsets();
    memset(rbuf,0,sizeof(rbuf)); ri=0; rfull=0;
    smA=smB=idle=idleFade=pp=ip=cpA=cpB=0;
    memset(tailA,0,sizeof(tailA)); memset(tailB,0,sizeof(tailB));
    mpu.update();
    gx=mpu.getAccX()*G; gy=mpu.getAccY()*G; gz=mpu.getAccZ()*G;
    lt=lmt=millis();
  }

  // Target ~250Hz loop. Busy-wait until at least 4ms since last frame.
  unsigned long now;
  do { now=millis(); } while(now-lt < 4);
  mpu.update();
  float dt=(now-lt)/1000.0f;
  if(dt<0.002f) dt=0.002f;   // clamp min (shouldn't happen)
  if(dt>0.05f)  dt=0.05f;    // clamp max — ignore stalls >50ms
  lt=now;

  // ── IMU read ──────────────────────────────────────────────
  float rx=mpu.getAccX()*G, ry=mpu.getAccY()*G, rz=mpu.getAccZ()*G;
  float gyrX=mpu.getGyroX(), gyrY=mpu.getGyroY(), gyrZ=mpu.getGyroZ();

  // ── Gravity LPF — time-constant based (rate-independent) ──
  // alpha = 1 - exp(-dt/tau). At dt=4ms, tau=1.5s → alpha≈0.0027
  // Tracks slow orientation changes; fast accelerations pass through.
  float galpha=1.0f-expf(-dt/GRAV_TAU);
  gx+=galpha*(rx-gx); gy+=galpha*(ry-gy); gz+=galpha*(rz-gz);

  // ── IMU acceleration (planar X+Y, rotation-gated) ──────────
  float dx=rx-gx, dy=ry-gy;
  float dm=sqrtf(dx*dx+dy*dy);
  bool rotating=(fabsf(gyrX)>GYRO_GATE||fabsf(gyrY)>GYRO_GATE||fabsf(gyrZ)>GYRO_GATE);
  bool accelActive=(!rotating && dm>AD*G);

  // ── Speed computation — three modes ────────────────────────
  // spdSrc 0 = IMU only  : dead reckoning, no GPS
  // spdSrc 1 = GPS only  : direct GPS speed, no IMU
  // spdSrc 2 = GPS+IMU   : GPS sets baseline each fix, IMU integrates between
  //
  // GPS is applied in the BLE handler (GPS_SPEED:) so vmp is already
  // updated the moment a fix arrives. Here we only run IMU integration.

  gpsAge += dt;

  if(cfg.spdSrc==0){
    // ── IMU ONLY ──────────────────────────────────────────────
    // Pure dead reckoning. Integrates acceleration → velocity.
    if(accelActive){
      float acc=(dm+pdm)*0.5f*dt*2.23694f; // m/s² → mph increment
      vmp=max(0.0f, vmp+acc*IMU_SCALE);
      pdm=dm;
    } else {
      pdm=0.0f;
      vmp*=expf(-IMU_DECAY*dt);            // decay when no motion
      if(vmp<0.05f) vmp=0.0f;
    }

  } else if(cfg.spdSrc==1){
    // ── GPS ONLY ──────────────────────────────────────────────
    // vmp is set directly in the BLE handler on each fix.
    // Between fixes: hold the value. If GPS goes stale, decay to zero.
    bool gpsOk=(gpsVmp>=0.0f && gpsAge<3.5f); // GPS-only stale check
    if(!gpsOk){
      vmp*=expf(-3.0f*dt);
      if(vmp<0.1f) vmp=0.0f;
    }
    // vmp already set to gpsVmp in BLE handler — nothing else to do

  } else {
    // ── GPS + IMU HYBRID ──────────────────────────────────────
    // GPS fix: sets vmp directly in BLE handler (immediate, no blend).
    // Between fixes: IMU integration nudges vmp for real-time feel.
    // IMU only adds forward — GPS corrects the baseline each fix.
    bool gpsOk=(gpsVmp>=0.0f && gpsAge<3.5f); // GPS-only stale check

    if(accelActive){
      float acc=(dm+pdm)*0.5f*dt*2.23694f;
      imuDelta += acc*IMU_SCALE;
      pdm=dm;
    } else {
      pdm=0.0f;
      imuDelta*=expf(-IMU_DECAY*dt);
    }
    // Only add IMU if it's pushing forward (not fighting a GPS stop)
    if(gpsOk && gpsVmp<0.5f) imuDelta=0.0f; // GPS says stopped — trust it
    vmp=max(0.0f, vmp+imuDelta*dt);

    // If GPS stale, decay without anchor
    if(!gpsOk && !accelActive){
      vmp*=expf(-1.5f*dt);
      if(vmp<0.1f) vmp=0.0f;
    }
  }

  vmp=max(0.0f,vmp);

  // 3D magnitude for idle detection
  float dm3=sqrtf(dx*dx+dy*dy+(rz-gz)*(rz-gz));

  // ── Idle ──────────────────────────────────────────────────
  ima=ima*0.9f+dm3*0.1f;
  if(ima>cfg.idleWakeG*G){lmt=now;idle=0;}
  if(cfg.idleArmed&&!idle&&(now-lmt)/1000UL>=cfg.idleTo) idle=1;
  idleFade=idle?min(1.0f,idleFade+IFADE*dt):max(0.0f,idleFade-IFADE*dt);

  // ── Roll ──────────────────────────────────────────────────
  float raw=mpu.getAngleX();
  rbuf[ri]=raw; ri=(ri+1)%cfg.ss; if(!ri)rfull=1;
  int cnt=rfull?cfg.ss:max(ri,1);
  float rs=0; for(int i=0;i<cnt;i++)rs+=rbuf[i];
  float roll=rs/cnt;

  // ── Heatmap ───────────────────────────────────────────────
  {
    bool out=fabsf(roll)>cfg.dz;
    if(out&&hmInDeadzone){(roll>0?cfg.hmRight:cfg.hmLeft)++;hmInDeadzone=0;}
    else if(!out) hmInDeadzone=1;
    if(fabsf(roll)>cfg.hmMaxRoll) cfg.hmMaxRoll=fabsf(roll);
  }

  // ── Phases ────────────────────────────────────────────────
  float sf=constrain(vmp/cfg.cap,0,1);
  pp+=((0.3f+(cfg.pspd-1)/19.0f*5.7f)*2*PI*dt); if(pp>2*PI)pp-=2*PI;
  ip+=((0.05f+(cfg.ispd-1)/19.0f*0.45f)*2*PI*dt); if(ip>2*PI)ip-=2*PI;
  // Base speed scales the entire curve — slow at cspd=1, fast at cspd=20, at ALL speeds
  float cBase=cfg.cspd/20.0f;
  float cspd=cBase*(3.0f+sf*57.0f)*dt;
  cpA+=cspd; if(cpA>=cfg.nA)cpA-=cfg.nA;
  cpB+=cspd; if(cpB>=cfg.nB)cpB-=cfg.nB;

  // ── Emergency ─────────────────────────────────────────────
  bool inD=cfg.emg&&(fabsf(roll)>=cfg.da);
  if(inD){
    bool fl=(now%cfg.fm)<(cfg.fm/2); uint8_t br=fl?255:0;
    if(cfg.dangGrad){
      for(int i=0;i<cfg.nA;i++){CRGB c=gcol(cfg.dangGdA,i,cfg.nA);A[i]=sc(c.r,c.g,c.b,br);}
      for(int i=0;i<cfg.nB;i++){CRGB c=gcol(cfg.dangGdA,i,cfg.nB);B[i]=sc(c.r,c.g,c.b,br);}
    } else {
      fill_solid(A,cfg.nA,sc(cfg.dr,cfg.dg,cfg.db,br));
      fill_solid(B,cfg.nB,sc(cfg.dr,cfg.dg,cfg.db,br));
    }
    FastLED.show(); goto send;
  }

  // ── Base render ───────────────────────────────────────────
  if(cfg.stat){
    int rlen=cfg.nA+cfg.nB;
    float off=fmodf(ip/(2*PI)*rlen,(float)rlen);
    if(cfg.rbowRevStat) off=fmodf(-off+rlen,(float)rlen);
    if(cfg.stMA==2) rainbowStrip(A,cfg.nA,255,off,rlen);
    else if(cfg.stMA==1) gradFull(A,cfg.nA,cfg.stGdA,255);
    else fill_solid(A,cfg.nA,sc(cfg.stAr,cfg.stAg,cfg.stAb,255));
    if(cfg.stMB==2){
      for(int i=0;i<cfg.nB;i++){
        uint8_t h=(uint8_t)(fmodf(off+cfg.nA+(cfg.nB-1-i),(float)rlen)/rlen*255);
        B[i]=hue(h,255);
      }
    } else if(cfg.stMB==1) gradFull(B,cfg.nB,cfg.stGdB,255);
    else fill_solid(B,cfg.nB,sc(cfg.stBr,cfg.stBg,cfg.stBb,255));
  } else {
    float tA=255,tB=255;
    if(cfg.carv){
      if(fabsf(roll)<=cfg.dz){tA=tB=cfg.carvNeutral;}
      else{
        float ramp=constrain((fabsf(roll)-cfg.dz)/(cfg.mx-cfg.dz),0,1);
        float dom=cfg.carvNeutral+ramp*(255.0f-cfg.carvNeutral), rec=cfg.carvNeutral*(1.0f-ramp);
        if(roll>0){tA=dom;tB=rec;}else{tA=rec;tB=dom;}
      }
    }
    smA+=cfg.sa*(tA-smA); smB+=cfg.sa*(tB-smB);
    uint8_t bA=(uint8_t)constrain(smA,0,255), bB=(uint8_t)constrain(smB,0,255);

    if(cfg.spd){
      float ff=sf, of=(cfg.fr>0.01f)?constrain((vmp-cfg.cap)/cfg.fr,0,1):0;
      int rlenS=cfg.nA+cfg.nB;
      float sOff=fmodf(ip/(2*PI)*rlenS,(float)rlenS);
      if(cfg.spdRbowA) rainbowStrip(A,cfg.nA,(uint8_t)(ff*bA),sOff,rlenS);
      if(cfg.spdRbowB) rainbowStrip(B,cfg.nB,(uint8_t)(ff*bB),sOff+cfg.nA,rlenS);
      if(cfg.spdRbowA&&cfg.spdRbowB) goto afterSpd;
      switch(cfg.eff){
        case FADE:
          doFade(A,cfg.nA,cfg.sgA,cfg.sgdA,cfg.cAr,cfg.cAg,cfg.cAb,cfg.ovr,cfg.ovg,cfg.ovb,ff,of,bA,cfg.dirOut,cfg.dirFwd);
          doFade(B,cfg.nB,cfg.sgB,cfg.sgdB,cfg.cBr,cfg.cBg,cfg.cBb,cfg.ovr,cfg.ovg,cfg.ovb,ff,of,bB,cfg.dirOut,cfg.dirFwd);
          break;
        case PULSE:
          doPulse(A,cfg.nA,cfg.sgA,cfg.sgdA,cfg.cAr,cfg.cAg,cfg.cAb,cfg.ovr,cfg.ovg,cfg.ovb,of,bA,cfg.pmin,cfg.pmax);
          doPulse(B,cfg.nB,cfg.sgB,cfg.sgdB,cfg.cBr,cfg.cBg,cfg.cBb,cfg.ovr,cfg.ovg,cfg.ovb,of,bB,cfg.pmin,cfg.pmax);
          break;
        case COMET:
          doComet(A,cfg.nA,cfg.sgA,cfg.sgdA,cfg.cAr,cfg.cAg,cfg.cAb,cfg.ovr,cfg.ovg,cfg.ovb,cpA,of,bA,cfg.ct,cfg.cometMode,tailA,dt);
          doComet(B,cfg.nB,cfg.sgB,cfg.sgdB,cfg.cBr,cfg.cBg,cfg.cBb,cfg.ovr,cfg.ovg,cfg.ovb,cpB,of,bB,cfg.ct,cfg.cometMode,tailB,dt);
          break;
        case ZONES:
          // Solid: fill_solid with zone colour. Gradient: gradFull with zone gradient.
          if(cfg.gA) gradFull(A,cfg.nA,cfg.gdA,cfg.masterBr);
          else fill_solid(A,cfg.nA,sc(cfg.cAr,cfg.cAg,cfg.cAb,cfg.masterBr));
          if(cfg.gB) gradFull(B,cfg.nB,cfg.gdB,cfg.masterBr);
          else fill_solid(B,cfg.nB,sc(cfg.cBr,cfg.cBg,cfg.cBb,cfg.masterBr));
          break;
      }
      afterSpd:;
    } else if(cfg.carv){
      int rlenC=cfg.nA+cfg.nB;
      float cOff=fmodf(ip/(2*PI)*rlenC,(float)rlenC);
      if(cfg.carvRbowA)      rainbowStrip(A,cfg.nA,bA,cOff,rlenC);
      else if(cfg.gA)         gradFull(A,cfg.nA,cfg.gdA,bA);
      else fill_solid(A,cfg.nA,sc(cfg.sAr,cfg.sAg,cfg.sAb,bA));
      if(cfg.carvRbowB)      rainbowStrip(B,cfg.nB,bB,cOff+cfg.nA,rlenC);
      else if(cfg.gB)         gradFull(B,cfg.nB,cfg.gdB,bB);
      else fill_solid(B,cfg.nB,sc(cfg.sBr,cfg.sBg,cfg.sBb,bB));
    } else {
      fill_solid(A,cfg.nA,CRGB::Black);
      fill_solid(B,cfg.nB,CRGB::Black);
    }
  }

  if(idleFade>0.001f) renderIdle();

  // ── Master brightness ─────────────────────────────────────
  if(cfg.masterBr<255){
    for(int i=0;i<cfg.nA;i++){A[i].r=(uint16_t)A[i].r*cfg.masterBr/255;A[i].g=(uint16_t)A[i].g*cfg.masterBr/255;A[i].b=(uint16_t)A[i].b*cfg.masterBr/255;}
    for(int i=0;i<cfg.nB;i++){B[i].r=(uint16_t)B[i].r*cfg.masterBr/255;B[i].g=(uint16_t)B[i].g*cfg.masterBr/255;B[i].b=(uint16_t)B[i].b*cfg.masterBr/255;}
  }
  FastLED.show();

  send:
  if(conn&&(now-stimer)>100){
    stimer=now;
    const char* en[]={"fade","pulse","comet","zones"};
    char buf[240];
    snprintf(buf,sizeof(buf),
      "{\"roll\":%.1f,\"mph\":%.1f,\"bA\":%d,\"bB\":%d,"
      "\"danger\":%s,\"emg\":%s,\"carvOn\":%s,\"spdOn\":%s,"
      "\"eff\":\"%s\",\"idle\":%s,\"idleArmed\":%s,"
      "\"hmL\":%d,\"hmR\":%d,\"hmMax\":%.1f,\"ver\":\"%s\"}",
      roll,vmp,
      (uint8_t)constrain(smA,0,255),(uint8_t)constrain(smB,0,255),
      inD?"true":"false",cfg.emg?"true":"false",
      cfg.carv?"true":"false",cfg.spd?"true":"false",
      en[cfg.eff],idle?"true":"false",cfg.idleArmed?"true":"false",
      (int)cfg.hmLeft,(int)cfg.hmRight,cfg.hmMaxRoll,FW_VERSION);
    sta->setValue((uint8_t*)buf,strlen(buf));sta->notify();
  }
}
