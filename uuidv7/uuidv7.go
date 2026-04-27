package uuidv7

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

// Generator 是一个并发安全的 UUID v7 生成器
type Generator struct {
	lastTs int64  // 上次时间戳（毫秒）
	seq    uint32 // 同毫秒内递增序列（12bit 使用）
}

// New 创建一个新的生成器
func New() *Generator {
	return &Generator{}
}

// Next 生成一个 UUID v7
func (g *Generator) Next() [16]byte {
	for {
		now := time.Now().UnixMilli()
		last := atomic.LoadInt64(&g.lastTs)

		// 时间回拨保护：保证单调不倒退
		if now < last {
			now = last
		}

		// 尝试更新当前时间窗口
		if atomic.CompareAndSwapInt64(&g.lastTs, last, now) {

			var seq uint32

			if now == last {
				// 同一毫秒：递增序列
				seq = atomic.AddUint32(&g.seq, 1)

				// 12bit 溢出（4096）→ 等待进入下一毫秒
				if seq > 0x0FFF {
					runtime.Gosched()
					continue
				}

			} else {
				// 新毫秒：重置序列
				seq = 0
				atomic.StoreUint32(&g.seq, 0)
			}

			return buildUUID(now, seq)
		}

		// CAS 失败：轻微退让，避免 CPU 热循环
		runtime.Gosched()
	}
}

func buildUUID(ts int64, seq uint32) [16]byte {
	var u [16]byte

	// 48-bit 时间戳（毫秒）
	u[0] = byte(ts >> 40)
	u[1] = byte(ts >> 32)
	u[2] = byte(ts >> 24)
	u[3] = byte(ts >> 16)
	u[4] = byte(ts >> 8)
	u[5] = byte(ts)

	// version(7) + seq 高 4bit
	u[6] = byte((0x7 << 4) | ((seq >> 8) & 0x0F))

	// seq 低 8bit
	u[7] = byte(seq)

	// 生成随机部分（用于填充剩余 62bit）
	var r [8]byte
	_, _ = rand.Read(r[:])

	// variant（10xxxxxx）
	u[8] = (r[0] & 0x3F) | 0x80

	// 剩余随机
	copy(u[9:], r[1:])

	return u
}

var hexStr = "0123456789abcdef"

// String 将 UUID 转为标准字符串形式：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func String(u [16]byte) string {
	var buf [36]byte

	j := 0
	for i := 0; i < 16; i++ {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			buf[j] = '-'
			j++
		}

		b := u[i]
		buf[j] = hexStr[b>>4]
		buf[j+1] = hexStr[b&0x0F]
		j += 2
	}

	return string(buf[:])
}

func (g *Generator) NextString() string {
	return String(g.Next())
}

// StringNoDash 输出 32位 hex（不带 -）
func StringNoDash(u [16]byte) string {
	var buf [32]byte

	j := 0
	for i := 0; i < 16; i++ {
		b := u[i]
		buf[j] = hexStr[b>>4]
		buf[j+1] = hexStr[b&0x0F]
		j += 2
	}

	return string(buf[:])
}

func (g *Generator) NextStringNoDash() string {
	return StringNoDash(g.Next())
}

// 定义一个密钥（16字节），生产环境请换成随机值
var secretKey = [16]byte{0x2d, 0x4e, 0x61, 0x70, 0x20, 0x42, 0x61, 0x6e, 0x61, 0x6e, 0x61, 0x21, 0x47, 0x6f, 0x67, 0x6f}

// NextEncryptedString 直接生成一个混淆后的 32位 字符串
func (g *Generator) NextEncryptedString() string {
	return StringNoDash(Encrypt(g.Next()))
}

// Encrypt 混淆 UUID：通过 XOR 和 字节置换 (独立函数，不依赖 Generator 状态)
func Encrypt(u [16]byte) [16]byte {
	var out [16]byte
	for i := 0; i < 16; i++ {
		out[i] = u[i] ^ secretKey[i]
	}
	// 混淆交换逻辑
	out[0], out[15] = out[15], out[0]
	out[1], out[10] = out[10], out[1]
	out[4], out[8] = out[8], out[4]
	return out
}

// Decrypt 还原 UUID
func Decrypt(u [16]byte) [16]byte {
	out := u
	out[4], out[8] = out[8], out[4]
	out[1], out[10] = out[10], out[1]
	out[0], out[15] = out[15], out[0]
	for i := 0; i < 16; i++ {
		out[i] ^= secretKey[i]
	}
	return out
}

// EncryptString 针对 Hex 字符串的加解密包装
func EncryptString(s string) (string, error) {
	if len(s) != 32 {
		return "", fmt.Errorf("invalid uuid string length")
	}
	src, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	var u [16]byte
	copy(u[:], src)
	return StringNoDash(Encrypt(u)), nil
}

func DecryptString(s string) (string, error) {
	if len(s) != 32 {
		return "", fmt.Errorf("invalid uuid string length")
	}
	src, err := hex.DecodeString(s)
	if err != nil {
		return "", err
	}
	var u [16]byte
	copy(u[:], src)
	return StringNoDash(Decrypt(u)), nil
}

// Global 全局一个 不用每次New
var Global *Generator

// 可以在 init 函数里处理
func init() {
	key := os.Getenv("UUID_SECRET_KEY")
	if key != "" {
		h := sha256.Sum256([]byte(key))
		copy(secretKey[:], h[:16])
	}
	Global = New()
}

// Next 在包中直接导出这些函数，连 Global 都不用写
func Next() [16]byte {
	return Global.Next()
}

func NextEncryptedString() string {
	return Global.NextEncryptedString()
}

func NextStringNoDash() string {
	return Global.NextStringNoDash()
}

func NextString() string {
	return String(Next())
}
