package whatsmeow_service

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_repository "github.com/evolution-foundation/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/safemap"
)

// fakeInstances answers the two repository calls the kill path makes and counts
// GetInstanceByID: ReconnectClient goes through it, so a count of zero proves
// nothing restarted the instance.
type fakeInstances struct {
	instance_repository.InstanceRepository
	gets atomic.Int32
}

func (f *fakeInstances) UpdateConnected(string, bool, string) error { return nil }
func (f *fakeInstances) UpdateQrcode(string, string) error           { return nil }
func (f *fakeInstances) GetInstanceByID(id string) (*instance_model.Instance, error) {
	f.gets.Add(1)
	return &instance_model.Instance{Id: id}, nil
}

func newTestService(t *testing.T) (whatsmeowService, *fakeInstances) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "dbdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{LogDirectory: filepath.Join(dir, "logs"), LogType: "json"}
	repo := &fakeInstances{}
	return whatsmeowService{
		config:             cfg,
		exPath:             dir,
		authStore:          &authStore{},
		instanceRepository: repo,
		killChannel:        safemap.New[chan bool](),
		clientPointer:      safemap.New[*whatsmeow.Client](),
		myClientPointer:    safemap.New[*MyClient](),
		userInfoCache:      cache.New(time.Minute, time.Minute),
		loggerWrapper:      logger_wrapper.NewLoggerManager(cfg),
	}, repo
}

// newUnpairedClient builds a client the way StartClient does for an instance
// that never paired (a fresh device, no JID), minus the network connection.
func newUnpairedClient(t *testing.T, w whatsmeowService, id string) (*MyClient, *ClientData) {
	t.Helper()
	container, err := w.authStore.open(context.Background(), w)
	if err != nil {
		t.Fatal(err)
	}
	client := whatsmeow.NewClient(container.NewDevice(), nil)
	kill := make(chan bool)
	w.killChannel.Set(id, kill)
	w.clientPointer.Set(id, client)
	cd := &ClientData{Instance: &instance_model.Instance{Id: id, Token: "token-" + id}}
	mycli := &MyClient{
		service:            &w,
		WAClient:           client,
		userID:             id,
		Instance:           cd.Instance,
		instanceRepository: w.instanceRepository,
		clientPointer:      w.clientPointer,
		myClientPointer:    w.myClientPointer,
		killChannel:        w.killChannel,
		userInfoCache:      w.userInfoCache,
		config:             w.config,
		loggerWrapper:      w.loggerWrapper,
		kill:               kill,
		done:               make(chan struct{}),
	}
	mycli.eventHandlerID = client.AddEventHandler(mycli.myEventHandler)
	w.myClientPointer.Set(id, mycli)
	return mycli, cd
}

func waitReturns(t *testing.T, what string, f func()) {
	t.Helper()
	ret := make(chan struct{})
	go func() { f(); close(ret) }()
	select {
	case <-ret:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// The production leak: an instance nobody pairs times out its QR codes over and
// over. Each timeout must end its client's goroutine instead of nesting a new
// StartClient inside it; hundreds of cycles must leave nothing behind.
func TestQRTimeoutCyclesLeaveNoGoroutines(t *testing.T) {
	w, _ := newTestService(t)
	const id = "unpaired"

	// Warm up the shared store and logger so their goroutines are in the baseline.
	mycli, cd := newUnpairedClient(t, w, id)
	waitReturns(t, "warm-up kill wait", func() {
		go mycli.signalKill()
		w.waitForKill(cd, mycli)
	})
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	for i := 0; i < 300; i++ {
		mycli, cd := newUnpairedClient(t, w, id)
		waitReturns(t, "kill wait", func() {
			go mycli.signalKill() // what teardownQR does at QR timeout
			w.waitForKill(cd, mycli)
		})
	}

	for _, n := range []int{w.killChannel.Len(), w.clientPointer.Len(), w.myClientPointer.Len()} {
		if n != 0 {
			t.Fatalf("a stopped client is still in the shared maps (%d entries)", n)
		}
	}
	time.Sleep(200 * time.Millisecond) // let the LoggedOut webhook goroutines finish
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Fatalf("goroutines grew from %d to %d over 300 QR timeouts", before, after)
	}
}

// A client being torn down after a reconnect must leave the newer client's
// entries alone, and report that it was superseded (no LoggedOut for it).
func TestReleaseKeepsTheNewerClient(t *testing.T) {
	w, _ := newTestService(t)
	old, _ := newUnpairedClient(t, w, "inst")
	newer, _ := newUnpairedClient(t, w, "inst")

	if !w.releaseClient(old) {
		t.Fatal("old client not reported as superseded")
	}
	if w.myClientPointer.Get("inst") != newer || w.clientPointer.Get("inst") != newer.WAClient || w.killChannel.Get("inst") != newer.kill {
		t.Fatal("releasing the old client removed the newer one")
	}
	if !old.stopped() || newer.stopped() {
		t.Fatal("wrong client stopped")
	}
}

// signalKill must never hang its caller: not after the client is released, not
// when the channel was closed by Delete, not when nobody is waiting.
func TestSignalKillNeverHangs(t *testing.T) {
	w, _ := newTestService(t)
	defer func(d time.Duration) { killSignalTimeout = d }(killSignalTimeout)
	killSignalTimeout = 50 * time.Millisecond

	released, _ := newUnpairedClient(t, w, "a")
	w.releaseClient(released)
	waitReturns(t, "signalKill on a released client", released.signalKill)

	closed, _ := newUnpairedClient(t, w, "b")
	close(closed.kill) // what instances.Delete does
	waitReturns(t, "signalKill on a closed channel", closed.signalKill)

	idle, _ := newUnpairedClient(t, w, "c")
	waitReturns(t, "signalKill with no receiver", idle.signalKill)
}

// A Disconnected before pairing stops the instance; it must not reconnect (that
// is what minted a new device and a new round of QR codes forever).
func TestDisconnectedBeforePairingStopsWithoutReconnecting(t *testing.T) {
	w, repo := newTestService(t)
	mycli, cd := newUnpairedClient(t, w, "unpaired")

	waitReturns(t, "kill wait after Disconnected", func() {
		mycli.myEventHandler(&events.Disconnected{})
		w.waitForKill(cd, mycli)
	})
	time.Sleep(100 * time.Millisecond)
	if n := repo.gets.Load(); n != 0 {
		t.Fatalf("the instance was restarted (%d instance lookups)", n)
	}
	if w.myClientPointer.Len() != 0 {
		t.Fatal("the stopped client is still registered")
	}
}

// A Disconnected from a client that was already replaced is ignored.
func TestDisconnectedFromAnOldClientIsIgnored(t *testing.T) {
	w, repo := newTestService(t)
	old, _ := newUnpairedClient(t, w, "inst")
	newer, _ := newUnpairedClient(t, w, "inst")

	old.myEventHandler(&events.Disconnected{})
	time.Sleep(100 * time.Millisecond)
	if repo.gets.Load() != 0 || newer.stopped() || w.myClientPointer.Get("inst") != newer {
		t.Fatal("an old client's Disconnected touched the current client")
	}
}

