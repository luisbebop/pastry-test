package main

import "fmt"
import "time"
import "flag"
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
    fmt.Printf("Forwarding message %s to Node %s.\n", msg.Key, next)
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
	//parsing parameters
	localIpPtr := flag.String("localip", "", "Your local ip")
	globalIpPtr := flag.String("globalip", "", "Your global ip")
	regionPtr := flag.String("region", "", "Your amazon region")
	nodeIpPtr := flag.String("nodeip", "", "A node ip to connect")
	nodeIdPtr := flag.String("nodeid", "", "At least 16 bytes to generate your node ID")
	msgIdPtr := flag.String("msgid", "", "At least 16 bytes do generate a message ID")
	msgCountPtr := flag.Int("msgcount", 1, "Number of messages to send")
	flag.Parse()
		
	//generating node id. We need at least 16 bytes
	id, err := pastry.NodeIDFromBytes([]byte(*nodeIdPtr))
	if err != nil {
		panic(err.Error())
	}
	
	//creating a new node and initializing a cluster
	node := pastry.NewNode(id, *localIpPtr, *globalIpPtr, *regionPtr, 5332)
	credentials := pastry.Passphrase("I S2 Gophers.")
	cluster := pastry.NewCluster(node,credentials)
	
	//registering an application and callbacks
	app := &PastryApplication{}
	cluster.RegisterCallback(app)
	
	//starting the cluster
	go func() {
		defer cluster.Stop()
		err := cluster.Listen()
		if err != nil {
			panic(err.Error())
		}
	}()
	
	fmt.Println("Pastry running node id:", id)
	
	//connecting to another node in the cluster
	go func () {
		if *nodeIpPtr != "" {
			fmt.Println("Joining node: ", *nodeIpPtr)
			cluster.Join(*nodeIpPtr, 5332)
		}
	}()
	
	//sending msgs to the cluster
	if *msgIdPtr != "" {
		go func() {
			time.Sleep (time.Second * 2)
			fmt.Println("sending message loop")
		
			idMsg, err := pastry.NodeIDFromBytes([]byte(*msgIdPtr))
			if err != nil {
				panic(err.Error())
			}
			purpose := byte(16)
			for i:= 0; i < *msgCountPtr; i++ {
				fmt.Println("sending message:", i+1)
				msg := cluster.NewMessage(purpose, idMsg, []byte("This is the body of the message."))
				err = cluster.Send(msg)
				if err != nil {
					panic(err.Error())
				}
				time.Sleep(time.Second * 1)
			}
		}()
	}
	
	select {}
}