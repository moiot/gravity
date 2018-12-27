package config_test

import (
	"github.com/juju/errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moiot/gravity/padder/config"
)

const confStr = `
[padder]
enable-delete = true
binlog-list = ["bin.001", "bin.002"]

[padder.mysql.target]
host = "mbk-dev01.oceanbase.org.cn"
username = "root"
password = ""
port = 3306
schema = "test"

[padder.mysql.start-position]
binlog-name= "bin.001"
binlog-pos= 1234`

var _ = Describe("config test", func() {
	It("read config normally", func() {
		config, err := config.CreateConfigFromString(confStr)
		Expect(err).To(BeNil())
		Expect(config).NotTo(BeNil())
		Expect(config.PadderConfig).NotTo(BeNil())
		padderConf := config.PadderConfig

		Expect(padderConf.EnableDelete).To(BeTrue())
		Expect(padderConf.BinLogList).To(BeEquivalentTo([]string{"bin.001", "bin.002"}))

		Expect(padderConf.MySQLConfig).NotTo(BeNil())

		Expect(padderConf.MySQLConfig.Target.Host).To(BeEquivalentTo("mbk-dev01.oceanbase.org.cn"))
		Expect(padderConf.MySQLConfig.Target.Username).To(BeEquivalentTo("root"))
		Expect(padderConf.MySQLConfig.Target.Password).To(BeEquivalentTo(""))
		Expect(padderConf.MySQLConfig.Target.Port).To(BeEquivalentTo(3306))
		Expect(padderConf.MySQLConfig.Target.Schema).To(BeEquivalentTo("test"))

		Expect(padderConf.MySQLConfig.StartPosition.BinLogFileName).To(BeEquivalentTo("bin.001"))
		Expect(padderConf.MySQLConfig.StartPosition.BinLogFilePos).To(BeEquivalentTo(1234))
		Expect(padderConf.MySQLConfig.StartPosition.BinlogGTID).To(BeEquivalentTo(""))

	})

	It("require binlog list", func() {
		conf, _ := config.CreateConfigFromString(confStr)
		padderConfig := conf.PadderConfig
		padderConfig.BinLogList = nil
		err := config.Validate(padderConfig)
		Expect(errors.IsNotValid(err)).To(BeTrue())
	})

	It("require mysql config", func() {
		conf, _ := config.CreateConfigFromString(confStr)
		padderConfig := conf.PadderConfig
		padderConfig.MySQLConfig = nil
		err := config.Validate(padderConfig)
		Expect(errors.IsNotValid(err)).To(BeTrue())
	})

	It("require mysql start position config", func() {
		conf, _ := config.CreateConfigFromString(confStr)
		padderConfig := conf.PadderConfig
		padderConfig.MySQLConfig.StartPosition = nil
		err := config.Validate(padderConfig)
		Expect(errors.IsNotValid(err)).To(BeTrue())
	})

	It("require mysql target config", func() {
		conf, _ := config.CreateConfigFromString(confStr)
		padderConfig := conf.PadderConfig
		padderConfig.MySQLConfig.Target = nil
		err := config.Validate(padderConfig)
		Expect(errors.IsNotValid(err)).To(BeTrue())
	})

	It("require mysql target schema", func() {
		conf, _ := config.CreateConfigFromString(confStr)
		padderConfig := conf.PadderConfig
		padderConfig.MySQLConfig.Target.Schema = ""
		err := config.Validate(padderConfig)
		Expect(errors.IsNotValid(err)).To(BeTrue())
	})
})
