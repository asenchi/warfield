package main

import (
	"fmt"
	"flag"
	"net"
	"io"
	"os"
	"godis"
	"url"
	"strings"
	"regexp"
	"strconv"
	"bufio"
)

const Version = `0.0.1`

// flags
var (
	laddr = flag.String("l", "127.0.0.1:7777", "the address to listen on (tcp)")
	saddr = flag.String("s", "127.0.0.1:514", "the address to send data (udp)")
	vflag = flag.Bool("v", false, "print version string")
)

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}

// Get CLOUD early and export our regex for testing
var (
	cloud = os.Getenv("CLOUD")
	Re    = regexp.MustCompile(`^<([^ ]+)>[0-9] [^ ]+ [a-zA-Z]+ [0-9]+ [^ ]+ [^ ]+ nginx: [^ ]+ [^ ]+ [^ ]+ \[.*\] (\".*\" [0-9]+ [0-9]+ \".*\" \".*\") (.*)$`)
)

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

	domainkey := strings.Join([]string{cloud, "domain", lr.domain}, ":")

	appid, err := r.Hget(domainkey, "app_id")
	if err != nil {
		return err
	}

	// Save our app_id to our LogRecord
	lr.appid = appid.String()

	appkey := strings.Join([]string{cloud, "app", appid.String()}, ":")

	token, err := r.Hget(appkey, "heroku_log_token")
	if err != nil {
		return err
	}

	// Save our token to our LogRecord
	lr.token = token.String()
	return nil
}

// The log message, properly formatted for us to send to syslog
func (lr *LogRecord) String() string {
	return "<" + strconv.Itoa(lr.code) + "> " + lr.token + "[nginx]: " + lr.message
}

// Our LogRecord cache
//
// A mapping of domains->LogRecords. We keep the last 100 around so we don't
// need to call out to redis each time. We'll probably build a proper cache or
// use memcache for this at some point.
type RecordCache struct {
	domains map[string]LogRecord // An array of LogRecords mapped by domain
}

// Lookup LogRecords in our cache using domain
func (rc *RecordCache) GetRecord(domain string) (LogRecord, bool) {
	record, ok := rc.domains[domain]

	if ok == false {
		// Return an new LogRecord if we don't have a corresponding
		// record in our cache.
		return LogRecord{domain: domain}, ok
	}
	return record, ok
}

// Cache logrecords using domain as a key
func (rc *RecordCache) SetRecord(lr LogRecord) {
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

	if *vflag {
		fmt.Println("warfield", Version)
		return
	}

	// Initialize our cache
	rc := &RecordCache{domains: make(map[string]LogRecord)}

	// env variable containing our redis address
	var (
		raddr = os.Getenv("HTTP_REDIS_MASTER_URL")
	)

	if raddr == "" {
		raddr = "redis://localhost:6379"
	}

	// Open connection to Redis
	redis, err := redisConn(raddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	// Resolve our TCPAddress
	addr, err := net.ResolveTCPAddr("tcp", *laddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	// Listen on our Addr:Port
	listen, err := net.ListenTCP("tcp", addr)
	fmt.Println("Now listening on", *laddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	// Connect to syslog
	syslog, err := net.Dial("udp", *saddr)
	fmt.Println("Now connected to", *saddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	for {
		// Accept connections
		conn, err := listen.AcceptTCP()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		go func() {
			// Serve forever
			err := recvServe(conn, syslog, rc, Re, redis)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}()
	}
}

func recvServe(r io.Reader, w io.Writer, rc *RecordCache, re *regexp.Regexp, redis *godis.Client) os.Error {
	nr, err := bufio.NewReaderSize(r, 4096)
	if err != nil {
		panic(err)
	}

	for {
		line, _, err := nr.ReadLine()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		// Cast the buffer to a string
		groups := re.FindStringSubmatch(string(line))
		// If we didn't match anything, grab next line
		if len(groups) == 0 {
			continue
		}

		// convert our code to an int
		code, _ := strconv.Atoi(groups[1])

		// groups[3] is 'domain'
		// If we don't have a domain, can't do anything, next
		if groups[3] == "-" || groups[3] == "" {
			continue
		}

		// Check our cache for our 'domain', return the corresponding LogRecord
		// or create a new one with 'domain' set.
		record, ok := rc.GetRecord(groups[3])
		if ok == false {
			// Finish filling out the new LogRecord
			record.raw = string(line)
			record.token = ""
			record.appid = ""
			record.code = code
			record.message = groups[2]

			// Match our LogRecord with tokens stored in Redis
			err := record.GetToken(redis)
			if err != nil {
				panic(err)
			}
			// Cache our LogRecord
			rc.SetRecord(record)
		}

		// Send to syslog
		_, err = w.Write([]byte(record.String()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}

	panic("not reached")
}
