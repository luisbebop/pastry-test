package main

import "fmt"
import "os"
import "secondbit.org/pastry"

type PastryApplication struct {
}

func (app *PastryApplication) OnError(err error) {
    panic(err.Error())
}

func (app *PastryApplication) OnDeliver(msg pastry.Message) {
    fmt.Println("Received message: ", msg)
}

func (app *PastryApplication) OnForward(msg *pastry.Message, next pastry.NodeID) bool {
    fmt.Printf("Forwarding message %s to Node %s.", msg.Key, next)
    return true // return false if you don't want the message forwarded
}

func (app *PastryApplication) OnNewLeaves(leaves []*pastry.Node) {
    fmt.Println("Leaf set changed: ", leaves)
}

func (app *PastryApplication) OnNodeJoin(node pastry.Node) {
    fmt.Println("Node joined: ", node.ID)
}

func (app *PastryApplication) OnNodeExit(node pastry.Node) {
    fmt.Println("Node left: ", node.ID)
}

func (app *PastryApplication) OnHeartbeat(node pastry.Node) {
    fmt.Println("Received heartbeat from ", node.ID)
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err.Error())
	}
	
	id, err := pastry.NodeIDFromBytes([]byte(hostname))
	if err != nil {
		panic(err.Error())
	}
	
	node := pastry.NewNode(id, os.Args[1], os.Args[2], os.Args[3], 5332)
	credentials := pastry.Passphrase("I S2 Gophers.")
	cluster := pastry.NewCluster(node,credentials)
	
	app := &PastryApplication{}
	cluster.RegisterCallback(app)
	
	go func() {
		defer cluster.Stop()
		err := cluster.Listen()
		if err != nil {
			panic(err.Error())
		}
	}()
	
	fmt.Println("Pastry running")
	fmt.Println("hostname: ", hostname)
	fmt.Println("id: ", id)
	
	arg := os.Args[4]
	if arg != "" {
		fmt.Println("Joining node: ", arg)
		cluster.Join(arg, 5332)
	}
	select {}
}