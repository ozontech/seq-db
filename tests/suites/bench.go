package suites

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/stretchr/testify/suite"
)

// sadly testify doesn't implement suite for benchmarks because of backward compatibility
// so I did minimal (copy-paste) implementation

type Bencher interface {
	B() *testing.B
	SetB(b *testing.B)
}

/*
in benchmarks also inherit from this struct
like this

type CustomBenchSuite struct {
	suite.Suite
	setup.Bench
}

type CustomBenchSuite struct {
	setup.BaseSuite
	setup.Bench
}
*/

type Bench struct {
	b *testing.B
}

func (s *Bench) B() *testing.B {
	return s.b
}

func (s *Bench) SetB(b *testing.B) {
	s.b = b
}

func (s *Bench) RunBench(name string, bench func()) {
	s.B().Run(name, func(b *testing.B) {
		defer func(b *testing.B) {
			s.SetB(b)
		}(s.B())
		s.SetB(b)

		b.ResetTimer()
		bench()
		b.StopTimer()
	})
}

func RunBench(b *testing.B, s Bencher) {
	methodFinder := reflect.TypeOf(s)
	suiteName := methodFinder.Elem().Name()

	s.SetB(b)

	if setupAllSuite, ok := s.(suite.SetupAllSuite); ok {
		setupAllSuite.SetupSuite()
	}
	defer func() {
		if tearDownAllSuite, ok := s.(suite.TearDownAllSuite); ok {
			tearDownAllSuite.TearDownSuite()
		}
	}()

	benchRegexp := regexp.MustCompile("^Benchmark")

	for i := 0; i < methodFinder.NumMethod(); i++ {
		method := methodFinder.Method(i)

		if !benchRegexp.MatchString(method.Name) {
			continue
		}

		b.Run(method.Name, func(b *testing.B) {
			defer func(b *testing.B) {
				s.SetB(b)
			}(s.B())
			s.SetB(b)

			if setupTestSuite, ok := s.(suite.SetupTestSuite); ok {
				setupTestSuite.SetupTest()
			}
			if beforeTestSuite, ok := s.(suite.BeforeTest); ok {
				beforeTestSuite.BeforeTest(suiteName, method.Name)
			}
			defer func() {
				if afterTestSuite, ok := s.(suite.AfterTest); ok {
					afterTestSuite.AfterTest(suiteName, method.Name)
				}

				if tearDownTestSuite, ok := s.(suite.TearDownTestSuite); ok {
					tearDownTestSuite.TearDownTest()
				}
			}()

			b.ResetTimer()
			method.Func.Call([]reflect.Value{reflect.ValueOf(s)})
			b.StopTimer()
		})
	}
}
