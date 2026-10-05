package main

// The suites run the same decode checks against fractions from different
// sources: sealed on the fly by the current code (default) or sealed by
// the era's own code for every legacy format version ("legacy",
// see testdata/legacy/seal-fraction.sh).

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/ozontech/seq-db/config"
)

// FractionDecoderTestSuite holds the decode checks. The source of the
// fraction is decided by the embedded suite: the current-version suite
// seals it with the local code, the legacy one seals it per version with
// the era's code via testdata/legacy/seal-fraction.sh.
type FractionDecoderTestSuite struct {
	suite.Suite

	fracName    string
	expectedVer config.BinaryDataVersion
}

func (s *FractionDecoderTestSuite) sealDocs(docsPerDocBlock int) {
	s.fracName = filepath.Join(s.T().TempDir(), "frac_000001")
	err := sealFraction(
		s.fracName,
		"",
		strings.NewReader(strings.Join(docLines(s.T(), testDocs), "\n")),
		docsPerDocBlock,
	)
	s.Require().NoError(err)
	s.expectedVer = config.CurrentFracVersion
}

func (s *FractionDecoderTestSuite) sealDocsEra(v config.BinaryDataVersion) {
	s.fracName = filepath.Join(s.T().TempDir(), fmt.Sprintf("frac_v%d", v))

	cmd := exec.Command("bash", "testdata/legacy/seal-fraction.sh", fmt.Sprintf("v%d", v), s.fracName)
	cmd.Stdin = strings.NewReader(strings.Join(docLines(s.T(), testDocs), "\n"))
	out, err := cmd.CombinedOutput()
	s.Require().NoError(err, "seal-fraction.sh failed:\n%s", out)

	s.expectedVer = v
}

func (s *FractionDecoderTestSuite) decodeAll() *collectedContent {
	content := &collectedContent{t: s.T()}
	err := decodeFraction(s.fracName, "", content)
	s.Require().NoError(err)
	return content
}

func (s *FractionDecoderTestSuite) TestDecode() {
	s.Run("info", s.checkInfo)
	s.Run("docs keep raw json and skip the system doc", s.checkDocs)
	s.Run("ids have contiguous lids", s.checkIDs)
	s.Run("tokens are sequential and sorted with correct freq and postings", s.checkTokens)
	s.Run("tokens without postings when requested alone", s.checkTokensWithoutPostings)
	s.Run("only selects sections", s.checkOnlyFiltersSections)
}

func (s *FractionDecoderTestSuite) checkInfo() {
	content := s.decodeAll()
	info := content.Info
	s.Require().NotNil(info)

	// the decoder must recognize the format version, not just happen to
	// read the files
	s.Equal(s.expectedVer, info.BinaryDataVer)

	s.Equal(uint32(3), info.DocsTotal)
	s.Positive(info.DocsOnDisk)
	s.Less(uint64(info.From), uint64(info.To))
}

func (s *FractionDecoderTestSuite) checkDocs() {
	content := s.decodeAll()

	s.Require().Len(content.Docs, 3)
	s.ElementsMatch(docLines(s.T(), testDocs), docTexts(content.Docs))
}

func (s *FractionDecoderTestSuite) checkIDs() {
	content := s.decodeAll()

	s.Require().Len(content.IDs, 4)
	for i, id := range content.IDs {
		s.Equal(uint32(i), id.LID)
	}
}

func (s *FractionDecoderTestSuite) checkTokens() {
	content := s.decodeAll()

	s.Require().Len(content.Tokens, len(testTokens))
	for i, tok := range content.Tokens {
		expected := testTokens[i]
		expected.TID = uint32(i + 1)
		expected.Kind = kindToken
		expected.LIDs = tok.LIDs
		s.Equal(expected, tok)

		s.Len(tok.LIDs, int(tok.Freq))
		for _, lid := range tok.LIDs {
			s.NotZero(lid)
			s.LessOrEqual(lid, uint32(3))
		}
	}
}

func (s *FractionDecoderTestSuite) checkTokensWithoutPostings() {
	content := &collectedContent{t: s.T()}
	err := decodeFraction(s.fracName, kindToken, content)
	s.Require().NoError(err)

	s.Require().Len(content.Tokens, len(testTokens))
	for i, tok := range content.Tokens {
		s.Equal(testTokens[i].Token, tok.Token)
		s.Equal(testTokens[i].Freq, tok.Freq)
		s.Empty(tok.LIDs)
	}
}

func (s *FractionDecoderTestSuite) checkOnlyFiltersSections() {
	content := &collectedContent{t: s.T()}
	err := decodeFraction(s.fracName, kindInfo+","+kindOffsets, content)
	s.Require().NoError(err)

	s.NotNil(content.Info)
	s.NotNil(content.Offsets)
	s.Empty(content.Docs)
	s.Empty(content.Tokens)
	s.Empty(content.IDs)
}

type CurrentVersionSuite struct {
	FractionDecoderTestSuite
}

func (s *CurrentVersionSuite) SetupTest() {
	s.sealDocs(defaultDocsPerDocBlock)
}

// LegacyVersionsSuite runs the same checks against a fraction sealed by
// the era's own code. The suite is instantiated per version by
// TestFractionDecoderLegacy; a future v7 in config/frac_version.go
// automatically adds v6 there. Versions older than v2 are not
// discoverable and are skipped.
type LegacyVersionsSuite struct {
	FractionDecoderTestSuite

	Version config.BinaryDataVersion
}

func (s *LegacyVersionsSuite) SetupTest() {
	s.sealDocsEra(s.Version)
}

func TestFractionDecoderCurrent(t *testing.T) {
	suite.Run(t, new(CurrentVersionSuite))
}

func TestFractionDecoderLegacy(t *testing.T) {
	for ver := config.BinaryDataV2; ver < config.CurrentFracVersion; ver++ {
		t.Run(fmt.Sprintf("v%d", ver), func(t *testing.T) {
			suite.Run(t, &LegacyVersionsSuite{Version: config.BinaryDataVersion(ver)})
		})
	}
}

func TestUnknownOnlySection(t *testing.T) {
	fracName := filepath.Join(t.TempDir(), "frac_000001")

	err := decodeFraction(fracName, "amogus", &collectedContent{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown section to decode: amogus")
}
