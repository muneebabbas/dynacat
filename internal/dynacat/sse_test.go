package dynacat

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type sseTestLeafWidget struct {
	widgetBase
	updateCount atomic.Int32
}

var sseTestLeafTemplate = mustParseTemplate("widget-base.html")

func (w *sseTestLeafWidget) initialize() error {
	w.withTitle("Leaf").withCacheDuration(time.Second)
	w.ContentAvailable = true
	return nil
}

func (w *sseTestLeafWidget) update(ctx context.Context) {
	w.updateCount.Add(1)
	w.scheduleNextUpdate()
}

func (w *sseTestLeafWidget) Render() template.HTML {
	return w.renderTemplate(w, sseTestLeafTemplate)
}

type sseBroadcastPayload struct {
	WidgetID uint64 `json:"widgetId"`
	HTML     string `json:"html"`
}

func newSSETestLeaf(due bool) *sseTestLeafWidget {
	w := &sseTestLeafWidget{
		widgetBase: widgetBase{
			ID:   widgetIDCounter.Add(1),
			Type: "sse-test-leaf",
		},
	}
	_ = w.initialize()
	if due {
		w.nextUpdate = time.Now().Add(-time.Second)
	} else {
		w.nextUpdate = time.Now().Add(time.Hour)
	}
	return w
}

func registerSSETestWidgets(app *application, pg *page, root widget) {
	app.widgetByID[root.GetID()] = root
	app.widgetToPage[root.GetID()] = pg

	switch v := root.(type) {
	case *splitColumnWidget:
		for _, child := range v.Widgets {
			registerSSETestWidgets(app, pg, child)
		}
	case *groupWidget:
		for _, child := range v.Widgets {
			registerSSETestWidgets(app, pg, child)
		}
	}
}

func newSSETestApp() (*application, *page, *sseClient) {
	pg := &page{Title: "Home", Slug: "home"}
	client := &sseClient{ch: make(chan string, 16)}
	app := &application{
		DynamicUpdateEnabled: true,
		widgetByID:           make(map[uint64]widget),
		widgetToPage:         make(map[uint64]*page),
		sseClients:           map[*sseClient]struct{}{client: {}},
	}
	return app, pg, client
}

func drainSSEMessages(t *testing.T, ch chan string) []sseBroadcastPayload {
	t.Helper()
	var msgs []sseBroadcastPayload
	for {
		select {
		case raw := <-ch:
			var p sseBroadcastPayload
			if err := json.Unmarshal([]byte(raw), &p); err != nil {
				t.Fatalf("failed to unmarshal SSE payload %q: %v", raw, err)
			}
			msgs = append(msgs, p)
		default:
			return msgs
		}
	}
}

func assertContainerBroadcastsOnlyDueLeaf(t *testing.T, container widget, dueChild, freshChild *sseTestLeafWidget) sseBroadcastPayload {
	t.Helper()

	app, pg, client := newSSETestApp()
	registerSSETestWidgets(app, pg, container)

	app.sseCheckAndPushUpdates(context.Background())

	msgs := drainSSEMessages(t, client.ch)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 SSE broadcast (due leaf only), got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].WidgetID != dueChild.GetID() {
		t.Fatalf("expected broadcast for leaf widget %d, got %d (container was %d)", dueChild.GetID(), msgs[0].WidgetID, container.GetID())
	}
	expectedAttr := fmt.Sprintf(`data-widget-id="%d"`, dueChild.GetID())
	if !strings.Contains(msgs[0].HTML, expectedAttr) {
		t.Errorf("expected leaf HTML to contain %s, got %q", expectedAttr, msgs[0].HTML)
	}
	if dueChild.updateCount.Load() != 1 {
		t.Errorf("expected due child updateCount=1, got %d", dueChild.updateCount.Load())
	}
	if freshChild.updateCount.Load() != 0 {
		t.Errorf("expected fresh child updateCount=0, got %d", freshChild.updateCount.Load())
	}

	return msgs[0]
}

func newSSETestSplitColumn(t *testing.T, children ...widget) *splitColumnWidget {
	t.Helper()
	container := &splitColumnWidget{
		widgetBase: widgetBase{
			ID:   widgetIDCounter.Add(1),
			Type: "split-column",
		},
		containerWidgetBase: containerWidgetBase{
			Widgets: widgets(children),
		},
	}
	if err := container.initialize(); err != nil {
		t.Fatalf("splitColumnWidget.initialize() error = %v", err)
	}
	return container
}

func newSSETestGroup(t *testing.T, children ...widget) *groupWidget {
	t.Helper()
	container := &groupWidget{
		widgetBase: widgetBase{
			ID:   widgetIDCounter.Add(1),
			Type: "group",
		},
		containerWidgetBase: containerWidgetBase{
			Widgets: widgets(children),
		},
	}
	if err := container.initialize(); err != nil {
		t.Fatalf("groupWidget.initialize() error = %v", err)
	}
	return container
}

func TestSSECheckAndPushUpdatesSkipsSplitColumnContainer(t *testing.T) {
	dueChild := newSSETestLeaf(true)
	freshChild := newSSETestLeaf(false)
	container := newSSETestSplitColumn(t, dueChild, freshChild)

	assertContainerBroadcastsOnlyDueLeaf(t, container, dueChild, freshChild)
}

func TestSSECheckAndPushUpdatesSkipsGroupContainerAndPreservesHideHeader(t *testing.T) {
	dueChild := newSSETestLeaf(true)
	freshChild := newSSETestLeaf(false)
	container := newSSETestGroup(t, dueChild, freshChild)

	msg := assertContainerBroadcastsOnlyDueLeaf(t, container, dueChild, freshChild)
	if strings.Contains(msg.HTML, `class="widget-header"`) {
		t.Errorf("expected group child to preserve hide-header, got %q", msg.HTML)
	}
}

func TestSSECheckAndPushUpdatesIgnoresRegisteredContainersDirectly(t *testing.T) {
	splitLeaf := newSSETestLeaf(true)
	splitContainer := newSSETestSplitColumn(t, splitLeaf)

	groupLeaf := newSSETestLeaf(true)
	groupContainer := newSSETestGroup(t, groupLeaf)

	// Register only the containers (without their children) to deterministically verify
	// sseCheckAndPushUpdates never updates or broadcasts containers directly.
	app, pg, client := newSSETestApp()
	for _, c := range []widget{splitContainer, groupContainer} {
		app.widgetByID[c.GetID()] = c
		app.widgetToPage[c.GetID()] = pg
	}

	app.sseCheckAndPushUpdates(context.Background())

	msgs := drainSSEMessages(t, client.ch)
	if len(msgs) != 0 {
		t.Fatalf("expected 0 SSE broadcasts for container widgets, got %d: %+v", len(msgs), msgs)
	}
	if splitLeaf.updateCount.Load() != 0 {
		t.Errorf("expected splitLeaf updateCount=0, got %d", splitLeaf.updateCount.Load())
	}
	if groupLeaf.updateCount.Load() != 0 {
		t.Errorf("expected groupLeaf updateCount=0, got %d", groupLeaf.updateCount.Load())
	}
}
