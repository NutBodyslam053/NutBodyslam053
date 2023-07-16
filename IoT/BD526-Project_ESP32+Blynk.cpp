// Set Blynk configuration
#define BLYNK_PRINT Serial
#define BLYNK_TEMPLATE_ID "TMPL66Ct5RtaA"
#define BLYNK_TEMPLATE_NAME "Quickstart Template"
#define BLYNK_AUTH_TOKEN "g5XeoArvNVnnrZ1zcZS8ZhAVNnOLjHhg"

// Include required libraries
#include <WiFi.h>
#include <BlynkSimpleEsp32.h>
#include <Adafruit_GFX.h>
#include <Adafruit_Sensor.h>
#include <FS.h>
#include <DHT.h>

// Define Wi-Fi credentials
char ssid[] = "Natto";
char pass[] = "natto053";

// Initialize Blynk timer
BlynkTimer timer;

// Define Blynk LED widget
WidgetLED led1(V1);

//==================================== LED Blink ====================================//
int led1Pin = 21;

void blinkLedWidget()
{
    digitalWrite(led1Pin, LOW);
    delay(500);
    digitalWrite(led1Pin, HIGH);
    delay(500);
}

//=============================== Switch Control (V0) ===============================//
int led2Pin = 27;

BLYNK_WRITE(V0)
{
  bool value = param.asInt();

  digitalWrite(led2Pin, value);
  Blynk.virtualWrite(V1, value);
  if (value == true) {
    Serial.printf("Switch Control = %d, (Switch On!)\n", value);
    Serial.println("--------------------------------------------");
  } else {
    Serial.printf("Switch Control = %d, (Switch Off!)\n", value);
    Serial.println("--------------------------------------------");
  }
}

//=============================== Button Control (V1) ===============================//
int btnPin = 25;
bool btnState = false;

void buttonLedWidget()
{
  bool isPressed = digitalRead(btnPin);
  
  if (isPressed != btnState) {
    if (isPressed) {
      led1.on();
      digitalWrite(led2Pin, HIGH);
      Serial.printf("Switch Value = %d, (Button is pressed!)\n", isPressed);
      Serial.println("--------------------------------------------");
    } else {
      led1.off();
      digitalWrite(led2Pin, LOW);
      Serial.printf("Switch Value = %d, (Button is released!)\n", isPressed);
      Serial.println("--------------------------------------------");
    }
    btnState = isPressed;
  }
}

//================================= Uptime (V2, V7) =================================//
void myTimerEvent_1sec()
{
  uint32_t uptime_1sec = millis() / 1000;
  Blynk.virtualWrite(V2, uptime_1sec);
}

void myTimerEvent_5sec()
{
  uint32_t uptime_5sec = millis() / 1000;
  Blynk.virtualWrite(V7, uptime_5sec);
}

//================================== DHT11 (V3, V4) =================================//
#define DHTPIN 26
#define DHTTYPE DHT11

DHT dht(DHTPIN, DHTTYPE);

void sendSensor()
{
  float t = dht.readTemperature(); // or dht.readTemperature(true) for Fahrenheit
  float h = dht.readHumidity();

  if (isnan(t) || isnan(h)) {
    Serial.println("Failed to read from DHT sensor!");
  } else {
    Serial.printf("Temperature: %.2f\u00B0C, Humidity: %.2f%%\n", t, h);
    Blynk.virtualWrite(V3, t);
    Blynk.virtualWrite(V4, h);
  }
}

//================================= PWM Control (V5) ================================//
BLYNK_WRITE(V5)
{
  uint8_t value = param.asInt();
  analogWrite(led2Pin, value);
  Serial.printf("LED Light Value = %d\n", value);
  Serial.println("--------------------------------------------");
}

//================================= Wi-Fi RSSI (V6) ================================//
void connectWiFi()
{
  WiFi.begin(ssid, pass);

  while (WiFi.status() != WL_CONNECTED) {
    Serial.println("Connecting to Wi-Fi...");
    delay(1000);
  }

  IPAddress localIP = WiFi.localIP();
  Serial.println("--------------------------------------------");
  Serial.printf("Connected to Wi-Fi IP address: %s\n", localIP.toString());
  Serial.println("--------------------------------------------");
}

void RSSI() 
{
  int8_t rssi = WiFi.RSSI();
  Serial.printf("RSSI: %d dBm\n", rssi);
  Serial.println("--------------------------------------------");
  Blynk.virtualWrite(V6, rssi);
}

//===================================== Setup ====================================//
void setup()
{
  Serial.begin(115200);
  Blynk.begin(BLYNK_AUTH_TOKEN, ssid, pass);

  pinMode(led1Pin, OUTPUT);
  pinMode(led2Pin, OUTPUT);
  pinMode(btnPin, INPUT);

  timer.setInterval(500L, blinkLedWidget);

  timer.setInterval(500L, buttonLedWidget);

  timer.setInterval(1000L, myTimerEvent_1sec);
  timer.setInterval(5000L, myTimerEvent_5sec);

  dht.begin();
  timer.setInterval(5000L, sendSensor);

  connectWiFi();
  timer.setInterval(5000L, RSSI);
}

//===================================== Loop ====================================//
void loop()
{
  Blynk.run();
  timer.run();
}