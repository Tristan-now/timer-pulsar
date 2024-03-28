package vo

import "fmt"

const successCode = 0

type Errorer interface {
	Error() error
}

type CodeMsg struct {
	code int32  `yaml:"Code"`
	Msg  string `yaml:"Msg"`
	Err  error  `yaml:"Err"`
}

func (c *CodeMsg) Error() error {
	if c.code == successCode {
		return nil
	}
	return fmt.Errorf("code: %d,msg: %s", c.code, c.Msg)
}
func NewCodeMsg(code int32, msg string) CodeMsg {
	cm := CodeMsg{
		code: code,
		Msg:  msg,
	}
	cm.Err = cm.Error()
	return cm
}

func NewCodeMsgWithErr(err error) CodeMsg { return CodeMsg{Err: err} }

type PageLimiter struct {
	Index int `json:"pageIndex" form:"pageIndex"`
	Size  int `json:"pageSize" form:"pageSize"`
}

func (p *PageLimiter) Get() (offset, limit int) {
	if p.Index <= 0 {
		p.Index = 1
	}
	if p.Size <= 0 {
		p.Size = 10
	}
	return (p.Index - 1) * p.Size, p.Size
}
