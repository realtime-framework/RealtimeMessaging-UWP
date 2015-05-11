using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace RealtimeFramework.Messaging.Ext {
    internal class Reconnector {
        private bool _isActive;
        private CancellationTokenSource cts;
        internal Reconnector() {
            this._isActive = false;
            this.cts = new CancellationTokenSource();
        }

        internal void Dispose() {
            cts.Cancel();
            this._isActive = false;
        }

        internal void Start(System.Threading.TimerCallback recCb, int interval) {
            if (this._isActive) {
                throw new Exception("reconnector already scheduled");
            }
            this.callback = recCb;
            this.interval = interval;
            Schedule(cts.Token, recCb, interval);
            this._isActive = true;
        }

        internal bool IsActive() {
            return this._isActive;
        }

        private async void Schedule(System.Threading.CancellationToken ct, System.Threading.TimerCallback recCb, int interval) {
            await Task.Delay(interval);
            if (_isActive) {
                recCb.Invoke(null);
            }
        }

        internal void ActionDone(bool reapeat) {
            if (reapeat) {
                Schedule(cts.Token, this.callback, this.interval);
            } else {
                this._isActive = false;
            }                       
        }

        public System.Threading.TimerCallback callback { get; set; }

        public int interval { get; set; }
    }
}
