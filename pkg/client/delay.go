package client

import (
	"math"
	"math/rand"
	"time"
)

var oldtime int = -1
var noiseRange float64 = 0
//if radio is y，noiserange is randfloat64 in [-y/(1+y),y/(1-y)]
func getnoiseRange(newtime int,ratio float64){
	//not locked, not necessary
	if newtime!=oldtime {
		if ratio >= 1{
			//[-1/2,4]
			noiseRange = float64(rand.Int63n(5) -1)/2
		} else if ratio <= 0{
			noiseRange = 0
		} else {
			//[-y/(1+y),y/(1-y)]
			noiseRange = float64(rand.Int63n(int64(10000*2*ratio)) -int64(10000*ratio*(1-ratio)))/(10000*(1-ratio*ratio))
			if noiseRange > 4{
				noiseRange = 4
			}
		}
		oldtime = newtime
	}
}

func (w *worker) Normal(loadStartTime time.Time)  {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 1 / (math.Sqrt(2*math.Pi) * w.std) * math.Pow(math.E, (-math.Pow((float64(timeNow) - w.mean), 2)/(2*math.Pow(w.std, 2))))
	tmp := 1/v*float64(w.delay)
	if tmp > 3 * math.Pow(10.0,10) {
		tmp = 3 * math.Pow(10.0,10)
	}
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Reverse_normal(loadStartTime time.Time)  {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 1 / (math.Sqrt(2*math.Pi) * w.std) * math.Pow(math.E, (-math.Pow((float64(timeNow) - w.mean), 2)/(2*math.Pow(w.std, 2))))
	tmp := v*float64(w.delay)
	if tmp > 3 * math.Pow(10.0,10) {
		tmp = 3 * math.Pow(10.0,10)
	}
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Noise_normal(loadStartTime time.Time)  {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 1 / (math.Sqrt(2*math.Pi) * w.std) * math.Pow(math.E, (-math.Pow((float64(timeNow) - w.mean), 2)/(2*math.Pow(w.std, 2))))
	tmp := 1/v*float64(w.delay)
	if tmp > 3 * math.Pow(10.0,10) {
		tmp = 3 * math.Pow(10.0,10)
	}
	getnoiseRange(timeNow,w.noiseRatio)
	noise := tmp*noiseRange
	time.Sleep(time.Duration(tmp+noise))
}

func (w *worker) Step(loadStartTime time.Time)  {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 3 - int(0.5 + float64(timeNow * 3.0 /w.period))
	tmp := int64(v)*w.delay
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Noise_step(loadStartTime time.Time)  {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 3 - int(0.5 + float64(timeNow * 3.0 /w.period))
	tmp := float64(v)*float64(w.delay)
	getnoiseRange(timeNow,w.noiseRatio)
	noise := tmp*noiseRange
	time.Sleep(time.Duration(tmp+noise))
}