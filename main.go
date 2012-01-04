package main

import (
	"fmt"
	"flag"
	"net"
	"os"
	"godis"
	"url"
	"strings"
	"regexp"
	"strconv"
)

// flags
var (
	laddr = flag.String("l", "127.0.0.1:7777", "the address to listen on")
	saddr = flag.String("s", "127.0.0.1:514", "the address to send data")
)

// Our log regex pattern, this can probably be cleaned up quite a bit
var Re = regexp.MustCompile(`^<([^ ]+)>[0-9] [^ ]+ [a-zA-Z]+ [0-9]+ [^ ]+ [^ ]+ nginx: [^ ]+ [^ ]+ [^ ]+ \[.*\] (\".*\" [0-9]+ [0-9]+ \".*\" \".*\") (.*)$`)

// Two env variables we need to make things work well
var (
	cloud    = os.Getenv("CLOUD")
	redisUrl = os.Getenv("HTTP_REDIS_MASTER_URL")
)

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

// Our core log structure
type LogRecord struct {
	raw     string // our raw log message
	token   string // logplex token
	appid   string // app_id
	code    int    // syslog code
	message string // meat of the log
	domain  string // domain the request was for
}

// Match our domain to app_id and token in Redis
func (lr *LogRecord) GetToken(r *godis.Client) interface{} {
	if lr.domain != "" {
		domainkey := strings.Join([]string{cloud, "domain", lr.domain}, ":")
		appid, err := r.Hget(domainkey, "app_id")
		if err != nil {
			return err
		}

		lr.appid = appid.String()

		appkey := strings.Join([]string{cloud, "app", appid.String()}, ":")
		token, err := r.Hget(appkey, "heroku_log_token")
		if err != nil {
			return err
		}
		lr.token = token.String()
		return nil
	}
	return os.NewError("requires domain")
}

// The log properly formatted message we send to syslog
func (lr *LogRecord) String() string {
	return "<" + strconv.Itoa(lr.code) + "> " + lr.token + "[nginx]: " + lr.message
}

// Our LogRecord cache
type RecordCache struct {
	domains map[string]LogRecord // An array of LogRecords mapped by domain
}

// Lookup LogRecords in our cache using domain
func (rc *RecordCache) GetRecord(domain string) (LogRecord, bool) {
	record, ok := rc.domains[domain]
	fmt.Printf("rc-record: %v\n\n\n", record)

	if ok == false {
		return LogRecord{}, ok
	}
	return record, ok
}

// Cache logrecords using domain as a key
func (rc *RecordCache) Cache(lr LogRecord) {
	fmt.Printf("rc-logrecord: %v\n", lr)
	fmt.Printf("rc-logrecord.domain: %v\n", lr.domain)
	rc.domains[lr.domain] = lr
}

// Establish a redis connection, return the godis.Client
func redisConn(s string) (c *godis.Client, err os.Error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	// url.Parse doesn't return ports, so split on ':'
	hostPort := strings.Split(u.Host, ":")

	// Create a slice with our strings
	cs := []string{"tcp", hostPort[0], hostPort[1]}

	// Result: 'tcp:Host:Port'
	connString := strings.Join(cs, ":")

	// Return a godis client
	c = godis.New(connString, 0, u.RawUserinfo)
	return c, nil
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	// Setup our UDP connection
	listen, err := setupReceiver()
	if err != nil {
		panic(err)
	}

	// Serve continuously
	readReceiverServe(listen)
}

// We resolve our address provided by -l and listen on that address
// Return a netConn object
func setupReceiver() (c *net.UDPConn, err os.Error) {
	addr, err := net.ResolveUDPAddr("udp", *laddr)
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Now listening on %v\n", addr)
	return conn, nil
}

// Receive a netConn as an argument and pull data off that UDP socket
// We receive data in 4k chunks and send it to a goroutine that parses the
// raw log into a LogRecord
func readReceiverServe(c *net.UDPConn) {
	for {
		buf := make([]byte, 4096)
		fmt.Printf("Size of our buffer: %v\n", len(buf))
		n, _, err := c.ReadFromUDP(buf)
		if err == os.EINVAL {
			panic(err)
		}
		if err != nil {
			fmt.Printf("unable to read buffer: %-v", err)
			continue
		}

		fmt.Printf("Number of bytes read: %v\n", n)
		buf = buf[:n]
		str := string(buf)
		fmt.Printf("We are sending: %v\n", str)

		parseRecord(str)
	}
	panic("not reached")
}

// Parse a string and create a LogRecord for it
func parseRecord(s string) {
	// Initialize our cache
	rc := &RecordCache{domains: make(map[string]LogRecord)}

	if redisUrl == "" {
		redisUrl = "redis://localhost:6379"
	}

	redis, err := redisConn(redisUrl)
	if err != nil {
		panic(err)
	}

	groups := Re.FindStringSubmatch(s)
	if len(groups) == 0 {
		panic(err)
	}
	// convert our code to an int
	code, _ := strconv.Atoi(groups[1])

	// groups[3] is 'domain'
	if groups[3] == "-" || groups[3] == "" {
		panic(err)
	}

	// Pull our record if we already have it cached
	record, ok := rc.GetRecord(groups[3])
	if ok == false {
		// create basic record
		record.raw = s
		record.token = ""
		record.appid = ""
		record.code = code
		record.message = groups[2]
		record.domain = groups[3]

		// Get our logrecords token
		err := record.GetToken(redis)
		if err != nil {
			panic(err)
		}
		// Cache our LogRecord
		rc.Cache(record)
	}

	fmt.Printf("\nRecord: %+v\n\n\n", record.String())
	// spit log.String() to syslog
	os.Exit(0)
}
