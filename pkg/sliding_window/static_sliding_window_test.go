package sliding_window_test

import (
	"sync"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"time"

	"github.com/moiot/gravity/mocks/pkg/sliding_window"
	. "github.com/moiot/gravity/pkg/sliding_window"
)

var _ = Describe("static sliding window test", func() {

	It("can handle normal sequence", func(done Done) {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		items := []*mock_sliding_window.MockWindowItem{}
		commitCalls := []*gomock.Call{}

		for i := 0; i < 10; i++ {
			item := mock_sliding_window.NewMockWindowItem(mockCtrl)
			item.EXPECT().SequenceNumber().Return(int64(i)).AnyTimes()
			item.EXPECT().EventTime().Return(time.Now()).AnyTimes()
			commitCalls = append(commitCalls, item.EXPECT().BeforeWindowMoveForward().Return().Times(1))

			items = append(items, item)
		}

		gomock.InOrder(commitCalls...)

		window := NewStaticSlidingWindow(100, "test")

		for i := 0; i < 10; i++ {
			window.AddWindowItem(items[i])
			window.AckWindowItem(items[i].SequenceNumber())
		}

		window.Close()
		close(done)
	})

	It("can handle totally reverse order", func(done Done) {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		items := []*mock_sliding_window.MockWindowItem{}
		commitCalls := []*gomock.Call{}
		for i := 0; i < 10; i++ {
			item := mock_sliding_window.NewMockWindowItem(mockCtrl)

			item.EXPECT().SequenceNumber().Return(int64(i)).AnyTimes()
			item.EXPECT().EventTime().Return(time.Now()).AnyTimes()

			commitCalls = append(commitCalls, item.EXPECT().BeforeWindowMoveForward().Times(1))

			items = append(items, item)
		}

		gomock.InOrder(commitCalls...)

		window := NewStaticSlidingWindow(100, "test")

		for i := 0; i < 10; i++ {
			window.AddWindowItem(items[i])
		}

		for i := 0; i < 10; i++ {
			window.AckWindowItem(items[9-i].SequenceNumber())
		}
		window.Close()
		close(done)
	})

	It("can handle multiple goroutine", func(done Done) {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		n := 200

		items := []*mock_sliding_window.MockWindowItem{}

		calls := []*gomock.Call{}
		for i := 0; i < n; i++ {
			item := mock_sliding_window.NewMockWindowItem(mockCtrl)
			item.EXPECT().SequenceNumber().Return(int64(i)).AnyTimes()
			calls = append(calls, item.EXPECT().BeforeWindowMoveForward())
			item.EXPECT().BeforeWindowMoveForward().Return().AnyTimes()
			item.EXPECT().EventTime().Return(time.Now()).AnyTimes()
			items = append(items, item)
		}

		gomock.InOrder(calls...)

		window := NewStaticSlidingWindow(n+1, "test")

		for i := 0; i < n; i++ {
			window.AddWindowItem(items[i])
		}

		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)

			go func(idx int) {
				defer wg.Done()

				window.AckWindowItem(items[idx].SequenceNumber())

			}(i)
		}

		// we need to wait here.
		wg.Wait()

		window.Close()
		close(done)
	})

	It("should show correct watermark", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		window := NewStaticSlidingWindow(10, "test")
		Expect(window.Watermark().ProcessTime.Unix()).Should(BeEquivalentTo(0))
		var watermark1 Watermark

		{
			item := mock_sliding_window.NewMockWindowItem(mockCtrl)
			item.EXPECT().SequenceNumber().Return(int64(1)).AnyTimes()
			item.EXPECT().BeforeWindowMoveForward().Return().AnyTimes()
			item.EXPECT().EventTime().Return(time.Now()).AnyTimes()
			start := time.Now()
			window.AddWindowItem(item)
			watermark1 = window.Watermark()
			Expect(watermark1.ProcessTime.Sub(start).Nanoseconds()).Should(BeNumerically("<=", time.Millisecond.Nanoseconds()))
			window.AckWindowItem(item.SequenceNumber())
			Consistently(func() time.Time { return window.Watermark().ProcessTime }).Should(Equal(watermark1.ProcessTime))
		}

		{
			item2 := mock_sliding_window.NewMockWindowItem(mockCtrl)
			item2.EXPECT().SequenceNumber().Return(int64(2)).AnyTimes()
			item2.EXPECT().BeforeWindowMoveForward().Return().AnyTimes()
			item2.EXPECT().EventTime().Return(time.Now()).AnyTimes()

			start2 := time.Now()
			window.AddWindowItem(item2)
			Consistently(func() time.Time { return window.Watermark().ProcessTime }).Should(Equal(watermark1.ProcessTime))
			window.AckWindowItem(item2.SequenceNumber())
			Eventually(func() bool { return window.Watermark().ProcessTime.After(watermark1.ProcessTime) }).Should(BeTrue())
			Expect(window.Watermark().ProcessTime.Sub(start2).Nanoseconds()).Should(BeNumerically("<=", time.Millisecond.Nanoseconds()))
		}
	})
})
