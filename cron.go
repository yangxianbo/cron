// Package cron implements a cron spec parser and runner.
package cron

import (
	"errors"
	//"fmt"
	//"github.com/go-spew/spew"
	"strconv"
	"sync"
	"time"

	"github.com/toolkits/container/nmap"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	Running     bool
	Lock        *sync.RWMutex
	IDchan      chan string
	EntriesMap  *nmap.SafeMap
	EntryChan   chan *Entry
	TimeOutChan chan *Entry
	CheckTMout  chan *EntryTimeout
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
//type EntryID string
type Extra struct {
	//Ignore timeout
	RunForce bool
	TimeOut  int64
	//Name is job's names
	Name string
	//if use @every,job will start when time.Now().Unix() % Delay == 0
	Delay int64
	// run job in this time range w(2-6)d(00-00-00~23-59-59)
	// calculate IncludeWeekDay
	IncludeTimeRange map[int][]*TimeSchedule
	// Do not run job in this time range 2018-01-01-00-00-00~2018-01-03-23-59-59
	ExcludeTimeRange map[int][]*TimeSchedule
	// just run once
	RunOnce bool
}
type CheckPoint struct {
	Year     int
	Schedule string
	RunOnce  bool
}
type TimeSchedule struct {
	Start string
	End   string
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID string
	// Schedule on which this job should be run.
	Schedule Schedule `json:"-"`
	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time
	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time
	// Job is the thing to run when the Schedule is activated.
	Job Job `json:"-"`
	//Lock
	Lock *sync.RWMutex `json:"-"`
	//Running status
	Status   string
	Runtimes int64
	// Running in specific years
	// RunYears []int
	Extra
}
type EntryTimeout struct {
	CheckEntry *Entry
	CheckPoint int64
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != "" }

// New returns a new Cron job runner.
func New() *Cron {
	init := &Cron{
		Running:     false,
		Lock:        new(sync.RWMutex),
		IDchan:      make(chan string, 10240),
		EntryChan:   make(chan *Entry, 10240),
		EntriesMap:  nmap.NewSafeMap(),
		TimeOutChan: make(chan *Entry, 10240),
		CheckTMout:  make(chan *EntryTimeout, 10240),
	}
	for i := 1; i <= 10240; i++ {
		init.IDchan <- strconv.Itoa(i)
	}
	return init
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() {
	f()
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func(), args *Extra) (string, error) {
	return c.AddJob(spec, FuncJob(cmd), args)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job, args *Extra) (string, error) {
	schedule, err := Parse(spec)
	if err != nil {
		return "", err
	}
	entryid := c.Schedule(schedule, cmd, args)
	if entryid == "" {
		err = errors.New("IDChan is full,can not add job")
		return "", err
	}
	return entryid, nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, args *Extra) string {
	if len(c.IDchan) > 0 {
		now := time.Now().Local()
		entry := &Entry{
			ID:       <-c.IDchan,
			Schedule: schedule,
			Job:      cmd,
			Prev:     now,
			Lock:     new(sync.RWMutex),
			Status:   "IDLE",
			Runtimes: 0,
		}
		if args == nil {
			args = new(Extra)
		}
		entry.Name = args.Name
		entry.TimeOut = args.TimeOut
		entry.RunForce = args.RunForce
		entry.Delay = args.Delay
		entry.RunOnce = args.RunOnce
		// entry.RunYears = args.RunYears
		// 20180122 增加执行时间范围
		if args.IncludeTimeRange == nil {
			entry.IncludeTimeRange = make(map[int][]*TimeSchedule)
		} else {
			entry.IncludeTimeRange = args.IncludeTimeRange
		}
		if args.ExcludeTimeRange == nil {
			entry.ExcludeTimeRange = make(map[int][]*TimeSchedule)
		} else {
			entry.ExcludeTimeRange = args.ExcludeTimeRange
		}
		if entry.Delay >= 60 {
			tmptime := schedule.Next(now).Unix()
			for {
				if tmptime%entry.Delay == 0 {
					entry.Next = time.Unix(tmptime, 0)
					break
				}
				tmptime++
			}
		} else if entry.Delay > 0 && entry.Delay < 60 {
			tmptime := schedule.Next(now).Unix()
			for {
				if tmptime%60 == 0 {
					entry.Next = time.Unix(tmptime, 0)
					break
				}
				tmptime++
			}
		} else {
			entry.Next = schedule.Next(now)
		}
		c.EntriesMap.Put(entry.ID, entry)
		c.EntryChan <- entry
		return entry.ID
	}
	return ""
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() *nmap.SafeMap {
	return c.EntriesMap
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id string) (*Entry, error) {
	if c.EntriesMap.ContainsKey(id) {
		if entrysf, ok := c.EntriesMap.Get(id); ok {
			entry := entrysf.(*Entry)
			return entry, nil
		}
	}
	err := errors.New("This Crontable not contain ID:" + id)
	return nil, err
}

// Remove an entry from being run in the future.
func (c *Cron) RemoveEntry(id string) bool {
	if c.EntriesMap.ContainsKey(id) {
		if entrysf, ok := c.EntriesMap.Get(id); ok {
			entry := entrysf.(*Entry)
			entry.Lock.Lock()
			entry.Status = "REMOVE"
			entry.Lock.Unlock()
		}
		return true
	}
	return false
}
func (c *Cron) EmptyEntry() {
	for _, id := range c.EntriesMap.Keys() {
		if entrysf, ok := c.EntriesMap.Get(id); ok {
			entry := entrysf.(*Entry)
			entry.Lock.Lock()
			entry.Status = "REMOVE"
			entry.Lock.Unlock()
		}
	}
}
func (c *Cron) Paused(id string) bool {
	if c.EntriesMap.ContainsKey(id) {
		if entrysf, ok := c.EntriesMap.Get(id); ok {
			entry := entrysf.(*Entry)
			entry.Lock.Lock()
			entry.Status = "PAUSED"
			entry.Lock.Unlock()
			return true
		}
	}
	return false
}
func (c *Cron) Restart(id string) bool {
	if c.EntriesMap.ContainsKey(id) {
		if entrysf, ok := c.EntriesMap.Get(id); ok {
			entry := entrysf.(*Entry)
			entry.Lock.Lock()
			entry.Next = entry.Schedule.Next(time.Now().Local())
			entry.Status = "IDLE"
			entry.Lock.Unlock()
			c.EntryChan <- entry
			return true
		}
	}
	return false
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.Lock.Lock()
	c.Running = true
	c.Lock.Unlock()
	go c.run()
	go c.checkTMout()
}
func (c *Cron) checkTMout() {
	for {
		if !c.Running {
			return
		}
		checkentry := <-c.CheckTMout
		go func(checkentry *EntryTimeout) {
			entry := checkentry.CheckEntry
			<-time.After(time.Duration(entry.TimeOut) * time.Second)
			if entry.Runtimes == checkentry.CheckPoint {
				c.TimeOutChan <- entry
			}
		}(checkentry)
	}
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	for {
		if !c.Running {
			return
		}
		//select {
		//case entry := <-c.EntryChan:
		entry := <-c.EntryChan
		go func(entry *Entry) {
			switch entry.Status {
			case "IDLE":
				entry.Lock.Lock()
				if entry.Status == "REMOVE" {
					entry.Lock.Unlock()
					id := entry.ID
					c.EntriesMap.Remove(id)
					c.IDchan <- id
					return
				}
				entry.Status = "PREPARE"
				entry.Runtimes++
				entry.Lock.Unlock()
				now := <-time.After(entry.Next.Sub(time.Now().Local()))
				// <-time.After(entry.Next.Sub(time.Now().Local()))
				if entry.Status != "PREPARE" {
					if entry.Status == "REMOVE" {
						id := entry.ID
						c.EntriesMap.Remove(id)
						c.IDchan <- id
						return
					}
					return
				}
				ptime := entry.Prev
				entry.Lock.Lock()
				entry.Status = "RUNNING"
				entry.Prev = entry.Next
				//entry.Next = entry.Schedule.Next(now)
				entry.Next = entry.Schedule.Next(entry.Next)
				// 检查执行周
				if len(entry.IncludeTimeRange) > 0 {
					if schedules, ok := entry.IncludeTimeRange[int(now.Weekday())]; ok {
						if calTimerange(true, schedules, ptime, now) {
							entry.Status = "IDLE"
							entry.Lock.Unlock()
							c.EntryChan <- entry
							return
						}
					}
				}
				// 检查停止执行范围
				if len(entry.ExcludeTimeRange) > 0 {
					if schedules, ok := entry.ExcludeTimeRange[now.Year()]; ok {
						if calTimerange(false, schedules, ptime, now) {
							entry.Status = "IDLE"
							entry.Lock.Unlock()
							c.EntryChan <- entry
							return
						}
					}
					if schedules, ok := entry.ExcludeTimeRange[0]; ok {
						if calTimerange(false, schedules, ptime, now) {
							entry.Status = "IDLE"
							entry.Lock.Unlock()
							c.EntryChan <- entry
							return
						}
					}
				}
				if entry.RunForce {
					go entry.Job.Run()
				} else {
					//timeout check
					if entry.TimeOut > 0 {
						runtime := entry.Runtimes
						checkentry := &EntryTimeout{CheckEntry: entry, CheckPoint: runtime}
						c.CheckTMout <- checkentry
					}
					entry.Job.Run()
				}
				//entry.Lock.Unlock()
				//timeout recover
				if time.Now().Local().Unix() > entry.Next.Unix() {
					for {
						entry.Next = entry.Schedule.Next(entry.Next)
						if time.Now().Local().Unix() < entry.Next.Unix() {
							break
						}
					}
				}
				//entry.Lock.Lock()
				entry.Status = "IDLE"
				entry.Lock.Unlock()
				c.EntryChan <- entry
				return
			}
		}(entry)
	}
	//}
	// time.Sleep(100 * time.Millisecond)
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	entrieslist := c.EntriesMap.Keys()
	for _, entryid := range entrieslist {
		c.RemoveEntry(entryid)
	}
	c.EntriesMap.Clear()
	c.Lock.Lock()
	c.Running = false
	c.Lock.Unlock()
}
