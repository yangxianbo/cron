package cron

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	IncludeLayOut = "15-04-05"
)

var (
	IncludeTimeReg *regexp.Regexp
	CheckPointReg  *regexp.Regexp
	ExcludeTimeReg *regexp.Regexp
)

func init() {
	IncludeTimeReg, _ = regexp.Compile("w\\((\\w)\\-(\\w)\\)d\\((\\w{2}\\-\\w{2}\\-\\w{2})\\~(\\w{2}\\-\\w{2}\\-\\w{2})\\)")
	CheckPointReg, _ = regexp.Compile("(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)")
	ExcludeTimeReg, _ = regexp.Compile("(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\~(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)")
}
func replaceSS(instr string) (outstr string) {
	outstr = strings.ToLower(instr)
	outstr = strings.Replace(outstr, "hh-mi-ss~hh-mi-ss", "00-00-00~23-59-59", -1)
	return
}
func CalculateIncludeTime(instr string) map[int][]*TimeSchedule {
	include := make(map[int][]*TimeSchedule)
	for _, pstr := range strings.Split(instr, ",") {
		pstr = replaceSS(pstr)
		fssi := IncludeTimeReg.FindStringSubmatchIndex(pstr)
		startWeekDayStr := string(IncludeTimeReg.ExpandString([]byte{}, "$1", pstr, fssi))
		endWeekDayStr := string(IncludeTimeReg.ExpandString([]byte{}, "$2", pstr, fssi))
		startTimeStr := string(IncludeTimeReg.ExpandString([]byte{}, "$3", pstr, fssi))
		endTimeStr := string(IncludeTimeReg.ExpandString([]byte{}, "$4", pstr, fssi))
		var err error
		var startWeekDay, endWeekDay int64
		startWeekDay, err = strconv.ParseInt(startWeekDayStr, 10, 64)
		if err != nil {
			continue
		} else {
			if startWeekDay < 0 || startWeekDay > 6 {
				continue
			}
		}
		endWeekDay, err = strconv.ParseInt(endWeekDayStr, 10, 64)
		if err != nil {
			continue
		} else {
			if endWeekDay < 0 || endWeekDay > 6 {
				continue
			}
		}
		var startTime, endTime time.Time
		startTime, err = time.Parse(IncludeLayOut, startTimeStr)
		if err != nil {
			continue
			// startTime, _ = time.Parse(IncludeLayOut, "00-00-00")
		}
		sh, sm, ss := startTime.Clock()
		endTime, err = time.Parse(IncludeLayOut, endTimeStr)
		if err != nil {
			continue
			// endTime, _ = time.Parse(IncludeLayOut, "23-59-59")
		}
		eh, em, es := endTime.Clock()
		trange := &TimeSchedule{
			Start: fmt.Sprintf("%v %v %v * * *", ss, sm, sh),
			End:   fmt.Sprintf("%v %v %v * * *", es, em, eh),
		}
		for i := int(startWeekDay); i >= int(startWeekDay) && i <= int(endWeekDay); i++ {
			if _, ok := include[i]; !ok {
				include[i] = []*TimeSchedule{}
				include[i] = append(include[i], trange)
			} else {
				include[i] = append(include[i], trange)
			}
		}
	}
	return include
}
func CalculateCheckPoint(instr string) (outarray []*CheckPoint) {
	for _, pstr := range strings.Split(instr, ",") {
		checkpoint := new(CheckPoint)
		checkpoint.RunOnce = true
		fssi := CheckPointReg.FindStringSubmatchIndex(pstr)
		years := strings.ToLower(string(CheckPointReg.ExpandString([]byte{}, "$1", pstr, fssi)))
		mons := strings.ToLower(string(CheckPointReg.ExpandString([]byte{}, "$2", pstr, fssi)))
		days := strings.ToLower(string(CheckPointReg.ExpandString([]byte{}, "$3", pstr, fssi)))
		hours := strings.ToLower(string(CheckPointReg.ExpandString([]byte{}, "$4", pstr, fssi)))
		mins := strings.ToLower(string(CheckPointReg.ExpandString([]byte{}, "$5", pstr, fssi)))
		seconds := strings.ToLower(string(CheckPointReg.ExpandString([]byte{}, "$6", pstr, fssi)))
		if years != "yyyy" {
			year, err := strconv.ParseInt(years, 10, 64)
			if err != nil {
				continue
			}
			if year <= 0 || year > 3000 || year < 2018 {
				continue
			}
			checkpoint.Year = int(year)
		} else {
			checkpoint.RunOnce = false
		}
		var Mon, Day, Hour, Min, Second string
		if mons != "mm" {
			mon, err := strconv.ParseInt(mons, 10, 64)
			if err != nil {
				continue
			}
			if mon < 0 || mon > 12 {
				continue
			}
			Mon = mons
		} else {
			Mon = "*"
			checkpoint.RunOnce = false
		}
		if days != "dd" {
			day, err := strconv.ParseInt(days, 10, 64)
			if err != nil {
				continue
			}
			if day < 1 || day > 31 {
				continue
			}
			Day = days
		} else {
			Day = "*"
			checkpoint.RunOnce = false
		}
		if hours != "hh" {
			hour, err := strconv.ParseInt(hours, 10, 64)
			if err != nil {
				continue
			}
			if hour < 0 || hour > 24 {
				continue
			}
			Hour = hours
		} else {
			Hour = "*"
			checkpoint.RunOnce = false
		}
		if mins != "mi" {
			min, err := strconv.ParseInt(mins, 10, 64)
			if err != nil {
				continue
			}
			if min < 0 || min > 59 {
				continue
			}
			Min = mins
		} else {
			Min = "*"
			checkpoint.RunOnce = false
		}
		if seconds != "ss" {
			second, err := strconv.ParseInt(seconds, 10, 64)
			if err != nil {
				continue
			}
			if second < 0 || second > 59 {
				continue
			}
			Second = seconds
		} else {
			Second = "*"
			checkpoint.RunOnce = false
		}
		checkpoint.Schedule = fmt.Sprintf("%v %v %v %v %v *", Second, Min, Hour, Day, Mon)
		outarray = append(outarray, checkpoint)
	}
	return outarray
}
func CalculateExcludeTime(instr string) map[int][]*TimeSchedule {
	exclude := make(map[int][]*TimeSchedule)
	for _, pstr := range strings.Split(instr, ",") {
		excludeTime := new(TimeSchedule)
		fssi := ExcludeTimeReg.FindStringSubmatchIndex(pstr)
		Syears := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$1", pstr, fssi)))
		Smons := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$2", pstr, fssi)))
		Sdays := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$3", pstr, fssi)))
		Shours := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$4", pstr, fssi)))
		Smins := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$5", pstr, fssi)))
		Sseconds := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$6", pstr, fssi)))
		Eyears := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$7", pstr, fssi)))
		Emons := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$8", pstr, fssi)))
		Edays := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$9", pstr, fssi)))
		Ehours := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$10", pstr, fssi)))
		Emins := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$11", pstr, fssi)))
		Eseconds := strings.ToLower(string(ExcludeTimeReg.ExpandString([]byte{}, "$12", pstr, fssi)))
		start, syear, err := fillTimeClock(Syears, Smons, Sdays, Shours, Smins, Sseconds)
		if err != nil {
			continue
		}
		end, eyear, err := fillTimeClock(Eyears, Emons, Edays, Ehours, Emins, Eseconds)
		if err != nil {
			continue
		}
		if syear != eyear {
			continue
		}
		excludeTime.Start = start
		excludeTime.End = end
		if _, ok := exclude[syear]; !ok {
			exclude[syear] = []*TimeSchedule{}
		}
		exclude[syear] = append(exclude[syear], excludeTime)
	}
	return exclude
}
func fillTimeClock(years, mons, days, hours, mins, seconds string) (schedule string, syear int, err error) {
	var year, mon, day, hour, min, second int64
	if years != "yyyy" {
		year, err = strconv.ParseInt(years, 10, 64)
		if err != nil {
			return
		}
		if year <= 0 || year > 3000 || year < 2018 {
			err = errors.New("Out of time range")
			return
		}
		syear = int(year)
	} else {
		syear = 0
	}
	if mons != "mm" {
		mon, err = strconv.ParseInt(mons, 10, 64)
		if err != nil {
			return
		}
		if mon < 0 || mon > 12 {
			err = errors.New("Out of time range")
			return
		}
	} else {
		mons = "*"
	}
	if days != "dd" {
		day, err = strconv.ParseInt(days, 10, 64)
		if err != nil {
			return "", 0, err
		}
		if day < 1 || day > 31 {
			err = errors.New("Out of time range")
			return
		}
	} else {
		days = "*"
	}
	if hours != "hh" {
		hour, err = strconv.ParseInt(hours, 10, 64)
		if err != nil {
			return "", 0, err
		}
		if hour < 0 || hour > 24 {
			err = errors.New("Out of time range")
			return
		}
	} else {
		hours = "*"
	}
	if mins != "mi" {
		min, err = strconv.ParseInt(mins, 10, 64)
		if err != nil {
			return "", 0, err
		}
		if min < 0 || min > 59 {
			err = errors.New("Out of time range")
			return
		}
	} else {
		mins = "*"
	}
	if seconds != "ss" {
		second, err = strconv.ParseInt(seconds, 10, 64)
		if err != nil {
			return "", 0, err
		}
		if second < 0 || second > 59 {
			err = errors.New("Out of time range")
			return
		}
	} else {
		seconds = "*"
	}
	schedule = fmt.Sprintf("%v %v %v %v %v *", seconds, mins, hours, days, mons)
	return
}
func calTimerange(isstart bool, schedules []*TimeSchedule, ptime, now time.Time) bool {
	for _, timerange := range schedules {
		var ss, es Schedule
		var err error
		if ss, err = Parse(timerange.Start); err != nil {
			return true
		}
		if es, err = Parse(timerange.End); err != nil {
			return true
		}
		if isstart {
			nowsecond := now.Hour()*3600 + now.Minute()*60 + now.Second()
			nx := ss.Next(ptime)
			nxsecond := nx.Hour()*3600 + nx.Minute()*60 + nx.Second()
			ex := es.Next(ptime)
			exsecond := ex.Hour()*3600 + ex.Minute()*60 + ex.Second()
			if nowsecond < nxsecond || nowsecond > exsecond {
				return true
			}
		} else {
			if now.Unix() >= ss.Next(ptime).Unix() && now.Unix() <= es.Next(ptime).Unix() {
				return true
			}
		}
	}
	return false
}
