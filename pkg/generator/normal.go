package generator

import (
	//"fmt"
	"math"
	"math/rand"
	"time"
)

type NormalDisb struct {
	Number
	lb       int64
	ub       int64
	interval int64
	ia float64
	iu float64
	start_minute int64
}



// NewExponential creats an exponential generator with percential and range.
func NewNormalDisb(lb int64, ub int64) *NormalDisb {
	a := 0.167
	u := 0.5
	m := time.Now().Unix()
	return &NormalDisb{
		lb:       lb,
		ub:       ub,
		interval: ub - lb + 1,
		ia: a,
		iu: u,
		start_minute: m/60,
	}
}

// Next implements the Generator Next interface.
func (e *NormalDisb) Next(r *rand.Rand) int64 {
	var tmp float64
	t1 := time.Now().Unix()/60
	tmp = r.Float64()
	t2 := (t1 - e.start_minute)/10
	p := 0.5 - 2 * math.Pow((tmp - 0.5),2)
	if(tmp > 0.5){
		p = 1 - p;
	}
	p = p - 0.1 * float64(t2%20)
	if(p < 0){
		p = p + 1
	}
	v := int64(p * float64(e.interval))
	//fmt.Println(tmp,p,t2,v)
	e.SetLastValue(v)  
	return v
}

