using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization.Json;

using RealtimeFramework.Messaging.Exceptions;

namespace RealtimeFramework.Messaging.Ext {
    class Connection {

        Windows.Networking.Sockets.MessageWebSocket _websocket = null;
        private Windows.Storage.Streams.DataWriter messageWriter;

        #region Methods - Public (3)

        //private Windows.Storage.Streams.DataWriter writer;

        public void Connect(string url)
        {
            Uri uri = null;

            string connectionId = Strings.RandomString(8);
            int serverId = Strings.RandomNumber(1, 1000);

            try
            {
                uri = new Uri(url);
            }
            catch (Exception)
            {
                throw new OrtcEmptyFieldException(String.Format("Invalid URL: {0}", url));
            }

            string prefix = uri != null && "https".Equals(uri.Scheme) ? "wss" : "ws";

            Uri connectionUrl = new Uri(String.Format("{0}://{1}:{2}/broadcast/{3}/{4}/websocket", prefix, uri.DnsSafeHost, uri.Port, serverId, connectionId));

            //
            // NOTE: For wss connections, must have a valid installed certificate
            // See: http://www.runcode.us/q/c-iphone-push-server
            //

            _websocket = new Windows.Networking.Sockets.MessageWebSocket();

            _websocket.Control.MessageType = Windows.Networking.Sockets.SocketMessageType.Utf8;
            _websocket.MessageReceived += _websocket_MessageReceived;
            _websocket.Closed += _websocket_Closed;


            _websocket.ConnectAsync(connectionUrl).Completed = new Windows.Foundation.AsyncActionCompletedHandler(
                (Windows.Foundation.IAsyncAction source, Windows.Foundation.AsyncStatus status) =>
            {
                //System.Diagnostics.Debug.WriteLine(":: connecting status " + status);
                if (status == Windows.Foundation.AsyncStatus.Canceled) {
                    _reconnectTimer.ActionDone(false);
                } else if (status == Windows.Foundation.AsyncStatus.Completed) {
                    //writer = new Windows.Storage.Streams.DataWriter(_websocket.OutputStream);
                    //writer.UnicodeEncoding = Windows.Storage.Streams.UnicodeEncoding.Utf8;
                    messageWriter = new Windows.Storage.Streams.DataWriter(_websocket.OutputStream);
                    var ev = OnOpened;
                    if (ev != null) {
                        Task.Factory.StartNew(() => ev());
                    }
                    //_reconnectTimer.ActionDone(true);
                } else if (status == Windows.Foundation.AsyncStatus.Error) {
                    var ev = OnError;
                    if (ev != null) {
                        Task.Factory.StartNew(() => ev(new OrtcNotConnectedException("Websocket has encountered an error.")));
                    }
                    _reconnectTimer.ActionDone(true);
                } else if (status == Windows.Foundation.AsyncStatus.Started) {

                } else {
                    //unknown state
                }
            });
            

            /*
            _websocket = new WebSocket(connectionUrl.AbsoluteUri);

            _websocket.Opened += new EventHandler(websocket_Opened);
            _websocket.Error += new EventHandler<SuperSocket.ClientEngine.ErrorEventArgs>(websocket_Error);
            _websocket.Closed += new EventHandler(websocket_Closed);
            _websocket.MessageReceived += new EventHandler<MessageReceivedEventArgs>(websocket_MessageReceived);

            _websocket.Open();
             */
        }

        void _websocket_Closed(Windows.Networking.Sockets.IWebSocket sender, Windows.Networking.Sockets.WebSocketClosedEventArgs args) {
            //System.Diagnostics.Debug.WriteLine(":: websocket closed");
            var ev = OnClosed;
            if (ev != null) {
                Task.Factory.StartNew(() => ev());
            }
        }

        void _websocket_MessageReceived(Windows.Networking.Sockets.MessageWebSocket sender, Windows.Networking.Sockets.MessageWebSocketMessageReceivedEventArgs args) {
            var ev = OnMessageReceived;
            if (ev != null) {
                try {
                    using (var reader = args.GetDataReader()) {
                        reader.UnicodeEncoding = Windows.Storage.Streams.UnicodeEncoding.Utf8;
                        var text = reader.ReadString(reader.UnconsumedBufferLength);
                        //System.Diagnostics.Debug.WriteLine(":: recived " + text);
                        Task.Factory.StartNew(() => ev(text));
                    }
                } catch {}
            }
        }

        public void Close()
        {
            if (_websocket != null)
            {
                _websocket.Close(1000, "Normal closure");
                /*
                if (_websocket.State != WebSocketState.Connecting)
                {
                    _websocket.Close();
                }*/
            }
        }

        public async void Send(string message)
        {
            if (_websocket != null && messageWriter != null)
            {
                try {
                    message = "\"" + message + "\"";
                    messageWriter.WriteString(message);
                    await messageWriter.StoreAsync();
                    //System.Diagnostics.Debug.WriteLine(":: send: " + message);
                } catch {/*
                    var ev = OnError;
                    if (ev != null) {
                        Task.Factory.StartNew(() => ev(new OrtcNotConnectedException("Unable to write to socket.")));
                    }*/
                }
                /*
                using (var writer = new Windows.Storage.Streams.DataWriter(_websocket.OutputStream)) {
                    writer.UnicodeEncoding = Windows.Storage.Streams.UnicodeEncoding.Utf8;
                    writer.WriteString(message);
                    writer.StoreAsync();
                    //await writer.FlushAsync();
                    System.Diagnostics.Debug.WriteLine(":: send: " + message);
                }
                */
            }
        }

        #endregion

        #region Methods - Private (1)

        /// <summary>
        /// Serializes the specified data.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        private string Serialize(object data)
        {
            string result = "";

            try
            {
                DataContractJsonSerializer serializer = new DataContractJsonSerializer(data.GetType());
                var stream = new System.IO.MemoryStream();
                serializer.WriteObject(stream, data);
                string jsonData = Encoding.UTF8.GetString(stream.ToArray(), 0, (int)stream.Length);
                //stream.Close();
                stream.Dispose();
                result = jsonData;
            }
            catch
            {
            }

            return result;
        }

        #endregion

        #region Delegates (4)

        public delegate void onOpenedDelegate();
        public delegate void onClosedDelegate();
        public delegate void onErrorDelegate(Exception error);
        public delegate void onMessageReceivedDelegate(string message);

        #endregion

        #region Events (4)

        public event onOpenedDelegate OnOpened;
        public event onClosedDelegate OnClosed;
        public event onErrorDelegate OnError;
        public event onMessageReceivedDelegate OnMessageReceived;
        private Reconnector _reconnectTimer;

        public Connection(Reconnector _reconnectTimer) {
            // TODO: Complete member initialization
            this._reconnectTimer = _reconnectTimer;
        }

        #endregion


        internal void Dispose() {
            if (_websocket != null)
                _websocket.Dispose();
        }
    }

    
}
