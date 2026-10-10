package masque

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	gotls "crypto/tls"
	"crypto/x509"
	"math/big"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet/tls"
)

const (
	WarpHost = "cloudflareaccess.com"
	WarpPath = "/"
)

func warpCertificate(der []byte) (*gotls.Certificate, error) {
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, errors.New("invalid WARP private key").Base(err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("the WARP private key is not an ECDSA key")
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
	}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &gotls.Certificate{Certificate: [][]byte{cert}, PrivateKey: key}, nil
}

// hasCustomTLSVerify reports whether the TLS config carries pcs
// (PinnedPeerCertSha256) or vcn (VerifyPeerCertByName) verification.
// GetTLSConfig always installs randCarrier.verifyPeerCert, so the
// callback being non-nil tells nothing: inspect the Rand carrier that
// GetTLSConfig stashes instead.
func hasCustomTLSVerify(tlsConfig *gotls.Config) bool {
	carrier, ok := tlsConfig.Rand.(*tls.RandCarrier)
	if !ok || carrier == nil {
		return false
	}
	return len(carrier.PinnedPeerCertSha256) > 0 || len(carrier.VerifyPeerCertByName) > 0
}

func useWarp(config *Config, tlsConfig *gotls.Config) error {
	if config.Warp == nil {
		return nil
	}
	cert, err := warpCertificate(config.Warp.PrivateKey)
	if err != nil {
		return err
	}
	tlsConfig.GetClientCertificate = func(*gotls.CertificateRequestInfo) (*gotls.Certificate, error) {
		return cert, nil
	}
	// pcs/vcn outrank the WARP leaf pin: when the user verifies by
	// pinned cert or peer-cert name (e.g. custom SNI serving a rotating
	// CDN leaf), verify only by pcs/vcn and skip the pin. The pin
	// applies only when neither is set. With no pin and no pcs/vcn,
	// standard CA verification applies (fail-closed for the self-signed
	// default endpoint). See XTLS/Xray-core#7115.
	if publicKey := config.Warp.PublicKey; len(publicKey) > 0 && !hasCustomTLSVerify(tlsConfig) {
		verify := tlsConfig.VerifyPeerCertificate
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return errors.New("the WARP endpoint sent no certificate")
			}
			leaf, err := x509.ParseCertificate(raw[0])
			if err != nil {
				return err
			}
			if !bytes.Equal(leaf.RawSubjectPublicKeyInfo, publicKey) {
				return errors.New("the WARP endpoint's key doesn't match \"publicKey\"")
			}
			if verify != nil {
				return verify(raw, chains)
			}
			return nil
		}
	}
	return nil
}
