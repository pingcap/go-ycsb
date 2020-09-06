package client

import (
	"math"
	"math/rand"
	"time"
)

var oldtime int = -1
var noiseRange float64 = 0
// If radio is y，noiseRange is rand float64 in [-y/(1+y),y/(1-y)]
func getnoiseRange(newtime int,ratio float64){
	// Not locked, not necessary
	if newtime!=oldtime {
		if ratio >= 1{
			// [-1/2,4]
			noiseRange = float64(rand.Int63n(5) -1)/2
		} else if ratio <= 0{
			noiseRange = 0
		} else {
			// [-y/(1+y),y/(1-y)]
			noiseRange = float64(rand.Int63n(int64(10000*2*ratio)) -int64(10000*ratio*(1-ratio)))/(10000*(1-ratio*ratio))
			if noiseRange > 4{
				noiseRange = 4
			}
		}
		oldtime = newtime
	}
}

func (w *worker) Normal(loadStartTime time.Time)  {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / (float64(w.expectedOps) * math.Sqrt(2*math.Pi) * w.std))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 1 / (math.Sqrt(2*math.Pi) * w.std) * math.Pow(math.E, (-math.Pow((float64(timeNow) - w.mean), 2)/(2*math.Pow(w.std, 2))))
	tmp := 1/v*float64(w.delay)
	if tmp > 3 * math.Pow(10.0,10) {
		tmp = 3 * math.Pow(10.0,10)
	}
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Noise_normal(loadStartTime time.Time)  {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / (float64(w.expectedOps) * math.Sqrt(2*math.Pi) * w.std))
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
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / float64(w.expectedOps))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 3 - int(float64(timeNow * 3.0 /w.period))
	tmp := int64(v)*w.delay
	time.Sleep(time.Duration(tmp))
}

func (w *worker) Noise_step(loadStartTime time.Time)  {
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / float64(w.expectedOps))
	timeNow := int(time.Now().Sub(loadStartTime).Minutes())%w.period
	v := 3 - int(float64(timeNow * 3.0 /w.period))
	tmp := float64(v)*float64(w.delay)
	getnoiseRange(timeNow,w.noiseRatio)
	noise := tmp*noiseRange
	time.Sleep(time.Duration(tmp+noise))
}




func (w *worker) MeituanRead(loadStartTime time.Time) {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	x := 24.0 * float64(timeNow) / float64(w.period) //Simulate 24 hours a day
	var y float64 = 0
	switch {
	case x <= 5:
		y = (6.0*x - x*x) / 12.0
	case x <= 7:
		y = 0.5
	case x <= 12:
		y = (50.0 - (x-5.0)*(x-5.0)) / 100.0
	case x <= 17:
		y = (36.0 - (x-18.0)*(x-18.0)) / 45000.0
	case x <= 19:
		y = 0.0005
	case x <= 24:
		y = (12.0*x - x*x) / 36000.0
	default:
		y = 1
	}
	Q := y * float64(w.expectedOps)
	if Q < float64(w.threadCount) {
		Q = float64(w.threadCount)
	}
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / Q)
	getnoiseRange(timeNow, w.noiseRatio)
	noise := int64(float64(w.delay) * noiseRange)
	time.Sleep(time.Duration(w.delay + noise))
}

func (w *worker) MeituanUpdate(loadStartTime time.Time) {
	timeNow := int(time.Now().Sub(loadStartTime).Minutes()) % w.period
	x := 24.0 * float64(timeNow) / float64(w.period) //Simulate 24 hours a day
	var y float64 = 0
	switch {
	case x <= 10:
		y = (12.0*x - x*x) / 36.0
	case x <= 14:
		y = 0.7
	case x <= 24:
		y = (36.0 - (x-18.0)*(x-18.0)) / 45.0
	default:
		y = 1
	}
	Q := y * float64(w.expectedOps)
	if Q < float64(w.threadCount) {
		Q = float64(w.threadCount)
	}
	w.delay = int64(float64(w.threadCount) * float64(time.Second) / Q)
	getnoiseRange(timeNow, w.noiseRatio)
	noise := int64(float64(w.delay) * noiseRange)
	time.Sleep(time.Duration(w.delay + noise))
}
