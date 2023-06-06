package main

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
	"gomysql2pg/cmd"
	"time"
)

func main() {
	color.Red("DDDDDDDDDDDDD      BBBBBBBBBBBBBBBBB               AAA                  GGGGGGGGGGGGG     OOOOOOOOO     DDDDDDDDDDDDD        ")
	color.Red("D::::::::::::DDD   B::::::::::::::::B             A:::A              GGG::::::::::::G   OO:::::::::OO   D::::::::::::DDD     ")
	color.Red("D:::::::::::::::DD B::::::BBBBBB:::::B           A:::::A           GG:::::::::::::::G OO:::::::::::::OO D:::::::::::::::DD   ")
	color.Red("DDD:::::DDDDD:::::DBB:::::B     B:::::B         A:::::::A         G:::::GGGGGGGG::::GO:::::::OOO:::::::ODDD:::::DDDDD:::::D  ")
	color.Red("  D:::::D    D:::::D B::::B     B:::::B        A:::::::::A       G:::::G       GGGGGGO::::::O   O::::::O  D:::::D    D:::::D ")
	color.Red("  D:::::D     D:::::DB::::B     B:::::B       A:::::A:::::A     G:::::G              O:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::BBBBBB:::::B       A:::::A A:::::A    G:::::G              O:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB:::::::::::::BB       A:::::A   A:::::A   G:::::G    GGGGGGGGGGO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::BBBBBB:::::B     A:::::A     A:::::A  G:::::G    G::::::::GO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::B     B:::::B   A:::::AAAAAAAAA:::::A G:::::G    GGGGG::::GO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D     D:::::DB::::B     B:::::B  A:::::::::::::::::::::AG:::::G        G::::GO:::::O     O:::::O  D:::::D     D:::::D")
	color.Red("  D:::::D    D:::::D B::::B     B:::::B A:::::AAAAAAAAAAAAA:::::AG:::::G       G::::GO::::::O   O::::::O  D:::::D    D:::::D ")
	color.Red("DDD:::::DDDDD:::::DBB:::::BBBBBB::::::BA:::::A             A:::::AG:::::GGGGGGGG::::GO:::::::OOO:::::::ODDD:::::DDDDD:::::D  ")
	color.Red("D:::::::::::::::DD B:::::::::::::::::BA:::::A               A:::::AGG:::::::::::::::G OO:::::::::::::OO D:::::::::::::::DD   ")
	color.Red("D::::::::::::DDD   B::::::::::::::::BA:::::A                 A:::::A GGG::::::GGG:::G   OO:::::::::OO   D::::::::::::DDD     ")
	color.Red("DDDDDDDDDDDDD      BBBBBBBBBBBBBBBBBAAAAAAA                   AAAAAAA   GGGGGG   GGGG     OOOOOOOOO     DDDDDDDDDDDDD        ")
	colorStr := color.New()
	colorStr.Add(color.BgMagenta)
	colorStr.Printf("Powered By: DBA Team Of Infrastructure Research Center")
	time.Sleep(5 * 100 * time.Millisecond)
	var log = logrus.New()
	fmt.Printf("\n")
	cmd.Execute()
	log.Info("[", time.Now().Format("2006-01-02 15:04:05.000000"), "]", "   Release version: v0.0.5")
}
