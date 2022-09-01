package main

import "github.com/TamaraDzambic/NASP-projekat/Engine"

func main() {
	config := Engine.NewConfig()
	engine := Engine.CreateEngine(config)
	Engine.Menu(engine)
	//mapa := map[string]int{}
	//f, _ := os.Open("Engine\\config.txt")
	//scanner := bufio.NewScanner(f)
	//for scanner.Scan() {
	//	line := scanner.Text()
	//	v := strings.Split(line, ":")
	//	i, _ := strconv.Atoi(v[1])
	//	mapa[v[0]]=i
	//	fmt.Println(v[0], v[1])
	//}

}