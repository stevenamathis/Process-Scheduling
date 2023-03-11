package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// Shortest Job First Priority (preemptive)
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		current         int
		prev            Process
		tempProcesses   = make([]Process, 0)
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	tempProcesses = append(tempProcesses, processes...)
	count := 0
	end := 0
	smallest := 999
	minPriority := 999
	changedProc := true
	turnaround := 0
	prev = processes[0]
	timeSpent := 0
	time := 0
	for time = 0; count != len(processes); time++ {

		waitingTime = 0
		turnaround = 0

		smallest = 999
		minPriority = 999
		for i := 0; i < len(processes); i++ {
			if processes[i].ArrivalTime <= int64(time) && tempProcesses[i].BurstDuration < int64(smallest) && tempProcesses[i].BurstDuration > 0 && tempProcesses[i].Priority < int64(minPriority) {
				smallest = int(tempProcesses[i].BurstDuration)
				minPriority = int(tempProcesses[i].Priority)
				current = i
			}
		}

		tempProcesses[current].BurstDuration--

		if tempProcesses[current].BurstDuration == 0 {
			count++
			end = time + 1
			lastCompletion = float64(end)
			waitingTime = int64(end) - int64(tempProcesses[current].ArrivalTime) - int64(processes[current].BurstDuration)
			turnaround = end - int(tempProcesses[current].ArrivalTime)

			schedule[current] = []string{
				fmt.Sprint(processes[current].ProcessID),
				fmt.Sprint(processes[current].Priority),
				fmt.Sprint(processes[current].BurstDuration),
				fmt.Sprint(processes[current].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(end),
			}
		}
		totalWait = totalWait + float64(waitingTime)
		totalTurnaround += float64(turnaround)
		if prev.ProcessID != processes[current].ProcessID {
			changedProc = true
		} else {
			changedProc = false
		}
		if !changedProc {
			timeSpent += 1
		} else if changedProc {
			gantt = append(gantt, TimeSlice{
				PID:   prev.ProcessID,
				Start: int64(time - timeSpent - 1),
				Stop:  0,
			})
			timeSpent = 0
		}
		prev = processes[current]
	}

	avgWait := totalWait / float64(len(processes))
	avgTurnaround := totalTurnaround / float64(len(processes))
	avgThroughput := float64(len(processes)) / float64(lastCompletion)

	gantt[0].Start = 0
	gantt = append(gantt, TimeSlice{
		PID:   processes[current].ProcessID,
		Start: int64(time - timeSpent - 1),
		Stop:  int64(time),
	})

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, avgWait, avgTurnaround, avgThroughput)

}

// shortest job first (preemptive)
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		current         int
		prev            Process
		tempProcesses   = make([]Process, 0)
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	tempProcesses = append(tempProcesses, processes...)
	count := 0
	end := 0
	smallest := 999
	changedProc := true
	turnaround := 0
	prev = processes[0]
	timeSpent := 0
	time := 0
	for time = 0; count != len(processes); time++ {

		waitingTime = 0
		turnaround = 0

		smallest = 999
		for i := 0; i < len(processes); i++ {
			if processes[i].ArrivalTime <= int64(time) && tempProcesses[i].BurstDuration < int64(smallest) && tempProcesses[i].BurstDuration > 0 {
				smallest = int(tempProcesses[i].BurstDuration)
				current = i
			}
		}

		tempProcesses[current].BurstDuration--

		if tempProcesses[current].BurstDuration == 0 {
			count++
			end = time + 1
			lastCompletion = float64(end)
			waitingTime = int64(end) - int64(tempProcesses[current].ArrivalTime) - int64(processes[current].BurstDuration)
			turnaround = end - int(tempProcesses[current].ArrivalTime)

			schedule[current] = []string{
				fmt.Sprint(processes[current].ProcessID),
				fmt.Sprint(processes[current].Priority),
				fmt.Sprint(processes[current].BurstDuration),
				fmt.Sprint(processes[current].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(end),
			}
		}
		totalWait = totalWait + float64(waitingTime)
		totalTurnaround += float64(turnaround)
		if prev.ProcessID != processes[current].ProcessID {
			changedProc = true
		} else {
			changedProc = false
		}
		if !changedProc {
			timeSpent += 1
		} else {
			gantt = append(gantt, TimeSlice{
				PID:   prev.ProcessID,
				Start: int64(time - timeSpent - 1),
				Stop:  0,
			})
			timeSpent = 0
		}
		prev = processes[current]
	}

	avgWait := totalWait / float64(len(processes))
	avgTurnaround := totalTurnaround / float64(len(processes))
	avgThroughput := float64(len(processes)) / float64(lastCompletion)

	gantt[0].Start = 0
	gantt = append(gantt, TimeSlice{
		PID:   processes[current].ProcessID,
		Start: int64(time - timeSpent - 1),
		Stop:  int64(time),
	})

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, avgWait, avgTurnaround, avgThroughput)

}

// Round Robin Scheduler
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		tempProcesses   = make([]Process, 0)
		count           int
		current         int
		quantum         int
		changedProc     bool
		prevProc        Process
		waitingTime     int64
		turnaround      int64
		endTime         int
		time            int
		timeSpent       int
		totalWait       int64
		totalTurnaround int64
		circuitVar      int
		count2          int
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	tempProcesses = append(tempProcesses, processes...)
	quantum = 2

	prevProc = processes[0]

	for time = 0; count != len(processes); time += quantum {

		fmt.Println(prevProc.ProcessID)
		fmt.Println(time)

		i := 0
		if circuitVar == len(processes) {
			circuitVar = 0
		}
		for i = circuitVar; i < len(processes); i++ {

			if processes[i].ArrivalTime <= int64(time) && tempProcesses[i].BurstDuration > 0 && prevProc.ProcessID != processes[i].ProcessID {
				current = i
				break
			}
		}
		fmt.Println(processes[current].ProcessID)

		tempProcesses[current].BurstDuration -= int64(quantum)

		if tempProcesses[current].BurstDuration <= 0 {
			count++
			time = time - int(0-tempProcesses[current].BurstDuration)
			endTime = time + 2

			waitingTime = int64(endTime) - processes[current].ArrivalTime - processes[current].BurstDuration
			turnaround = int64(endTime) - processes[current].ArrivalTime

			schedule[current] = []string{
				fmt.Sprint(processes[current].ProcessID),
				fmt.Sprint(processes[current].Priority),
				fmt.Sprint(processes[current].BurstDuration),
				fmt.Sprint(processes[current].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(endTime),
			}
		}

		if prevProc.ProcessID != processes[current].ProcessID {
			changedProc = true
		} else {
			changedProc = false
		}

		if changedProc {
			gantt = append(gantt, TimeSlice{
				PID:   prevProc.ProcessID,
				Start: int64(time - timeSpent - 2),
				Stop:  0,
			})
			timeSpent = 0
		} else {
			timeSpent += 1
		}

		prevProc = processes[current]
		circuitVar++

		totalWait += waitingTime
		totalTurnaround += turnaround

		count2++

	}

	gantt[0].Start = 0
	gantt = append(gantt, TimeSlice{
		PID:   processes[current].ProcessID,
		Start: int64(time - timeSpent - 2),
		Stop:  int64(time),
	})

	avgWait := float64(totalWait) / float64(count)
	avgTurnaround := float64(totalTurnaround) / float64(count)
	throughput := float64(count) / float64(time)

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, avgWait, avgTurnaround, throughput)

}

//endregion

func RemoveIndex(s []Process, index int) []Process {
	return append(s[:index], s[index+1:]...)
}

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
