package server

import (
	"fmt"

	"github.com/siddontang/go-mysql/mysql"
)

func (c *Conn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{}
	}

	r.Status |= c.status

	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.WritePacket(data)
}

func (c *Conn) writeError(e error) error {
	var m *mysql.MyError
	var ok bool
	if m, ok = e.(*mysql.MyError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.WritePacket(data)
}

func (c *Conn) writeEOF() error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(c.status), byte(c.status>>8))
	}

	return c.WritePacket(data)
}

func (c *Conn) writeResultset(r *mysql.Resultset) error {
	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data := make([]byte, 4, 1024)

	data = append(data, columnLen...)
	if err := c.WritePacket(data); err != nil {
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	return nil
}

func (c *Conn) writeFieldList(fs []*mysql.Field) error {
	data := make([]byte, 4, 1024)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}
	return nil
}

type noResponse struct{}

func (c *Conn) writeValue(value interface{}) error {
	switch v := value.(type) {
	case noResponse:
		return nil
	case error:
		return c.writeError(v)
	case nil:
		return c.writeOK(nil)
	case *mysql.Result:
		if v != nil && v.Resultset != nil {
			return c.writeResultset(v.Resultset)
		} else {
			return c.writeOK(v)
		}
	case []*mysql.Field:
		return c.writeFieldList(v)
	case *Stmt:
		return c.writePrepare(v)
	default:
		return fmt.Errorf("invalid response type %T", value)
	}
}
