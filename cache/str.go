package cache

import (
	"net"
	"strings"
	"unsafe"
)

// String converts byte slice to string.
func String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Bytes converts string to byte slice.
func Bytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// Join concatenates the elements of its first argument to create a single string. The separator
// string sep is placed between elements in the resulting string.
func Join(elems []string, sep string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return elems[0]
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(elems[0])
	for _, s := range elems[1:] {
		b.WriteString(sep)
		b.WriteString(s)
	}
	return b.String()
}

// GetOutBoundIp 获取本地外网IP
func GetOutBoundIp() string {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err == nil {
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		ip := strings.Split(localAddr.String(), ":")[0]
		return ip
	}
	return GetLocalIp()
}

// GetLocalIp 获取本地IP
func GetLocalIp() string {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			addrs, _ := netInterfaces[i].Addrs()
			for _, address := range addrs {
				if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						return ipnet.IP.String()
					}
				}
			}
		}
	}
	return ""
}
