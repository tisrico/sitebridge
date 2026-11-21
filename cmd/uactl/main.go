// Command uactl is a tiny OPC UA client for poking at sitebridge:
//
//	uactl -e opc.tcp://127.0.0.1:4840 read  'ns=1;s=bess1.soc'
//	uactl -e opc.tcp://127.0.0.1:4840 write 'ns=1;s=bess1.power_setpoint.setpoint' -150
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

func main() {
	endpoint := flag.String("e", "opc.tcp://127.0.0.1:4840", "OPC UA endpoint")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: uactl [-e endpoint] read <nodeid>... | write <nodeid> <value>")
	}
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := opcua.NewClient(*endpoint, opcua.SecurityMode(ua.MessageSecurityModeNone))
	if err != nil {
		fail(err)
	}
	if err := c.Connect(ctx); err != nil {
		fail(err)
	}
	defer c.Close(ctx)

	switch args[0] {
	case "read":
		req := &ua.ReadRequest{}
		for _, a := range args[1:] {
			req.NodesToRead = append(req.NodesToRead, &ua.ReadValueID{NodeID: mustID(a), AttributeID: ua.AttributeIDValue})
		}
		resp, err := c.Read(ctx, req)
		if err != nil {
			fail(err)
		}
		for i, r := range resp.Results {
			var v any
			if r.Value != nil {
				v = r.Value.Value()
			}
			fmt.Printf("%-45s %-12v %v  %s\n", args[i+1], v, r.Status, r.SourceTimestamp.Format(time.RFC3339Nano))
		}
	case "write":
		if len(args) != 3 {
			flag.Usage()
			os.Exit(2)
		}
		f, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			fail(err)
		}
		resp, err := c.Write(ctx, &ua.WriteRequest{NodesToWrite: []*ua.WriteValue{{
			NodeID: mustID(args[1]), AttributeID: ua.AttributeIDValue,
			Value: &ua.DataValue{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(f)},
		}}})
		if err != nil {
			fail(err)
		}
		fmt.Println(resp.Results[0])
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func mustID(s string) *ua.NodeID {
	id, err := ua.ParseNodeID(s)
	if err != nil {
		fail(err)
	}
	return id
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
