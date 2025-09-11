package main

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

func main() {
	// first case
	st := sync.Map{}
	set1, _ := st.LoadOrStore("set1", sets.New[string]())
	set1.(sets.Set[string]).Insert("1", "2", "3")
	set1y, _ := st.Load("set1")
	fmt.Println(set1y.(sets.Set[string]).HasAll("1", "2", "3"))

	// second case
	set1, _ = st.LoadOrStore("set1", sets.New[string]())
	set1.(sets.Set[string]).Insert("4", "5", "6")
	set1y, _ = st.Load("set1")
	fmt.Println(set1y.(sets.Set[string]).HasAll("1", "2", "3", "4", "5", "6"))
}
