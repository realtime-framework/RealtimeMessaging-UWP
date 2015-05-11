using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using RealtimeFramework.Messaging.Ext;
using RealtimeFramework.Messaging.Exceptions;

namespace RealtimeFramework.Messaging {
    internal class Client {
        internal OrtcClient context;

        internal bool _isConnecting;
        internal bool _alreadyConnectedFirstTime;
        internal bool _stopReconnecting;
        internal bool _callDisconnectedCallback;
        internal bool _waitingServerResponse;
        internal List<KeyValuePair<string, string>> _permissions;
        internal ConcurrentDictionary<string, ChannelSubscription> _subscribedChannels;
        internal ConcurrentDictionary<string, ConcurrentDictionary<int, BufferedMessage>> _multiPartMessagesBuffer;
        internal Connection _webSocketConnection;
        internal DateTime? _reconnectStartedAt;
        internal DateTime? _lastKeepAlive; // Holds the time of the last keep alive received from the server
        
        internal /*Timer*/ Reconnector _reconnectTimer; // Timer to reconnect
        internal Timer _heartbeatTimer;
        internal Timer _connectionTimer;
        private MsgProcessor _msgProcessor;
        private int _gotOnOpenCount;
        private bool _connectionTimerIsDisposed;

        internal Client(OrtcClient ortcClient) {
            this.context = ortcClient;
            _isConnecting = false;
            _alreadyConnectedFirstTime = false;
            _stopReconnecting = false;
            _callDisconnectedCallback = false;
            _waitingServerResponse = false;

            _gotOnOpenCount = 0;

            _permissions = new List<KeyValuePair<string, string>>();

            _lastKeepAlive = null;
            _reconnectStartedAt = null;
            _reconnectTimer = new Reconnector();//= null;

            _subscribedChannels = new ConcurrentDictionary<string, ChannelSubscription>();
            _multiPartMessagesBuffer = new ConcurrentDictionary<string, ConcurrentDictionary<int, BufferedMessage>>();

            TimerCallback hbCb = this._heartbeatTimer_Elapsed;
            _heartbeatTimer = new Timer(hbCb, null, Timeout.Infinite, context.HeartbeatTime);
            TimerCallback cnCB = this._connectionTimer_Elapsed;
            _connectionTimer = new Timer(cnCB, null, System.Threading.Timeout.Infinite, Constants.SERVER_HB_COUNT);
            _connectionTimerIsDisposed = false;

            //_webSocketConnection = new Connection(_reconnectTimer);
            //_webSocketConnection.OnOpened += new Connection.onOpenedDelegate(_webSocketConnection_OnOpened);
            //_webSocketConnection.OnClosed += new Connection.onClosedDelegate(_webSocketConnection_OnClosed);
            //_webSocketConnection.OnError += new Connection.onErrorDelegate(_webSocketConnection_OnError);
            //_webSocketConnection.OnMessageReceived += new Connection.onMessageReceivedDelegate(_webSocketConnection_OnMessageReceived);

            _msgProcessor = new MsgProcessor(this);
        }

        private void _webSocketConnection_OnOpened() {
            sendValidateCommand();
        }

        private void _webSocketConnection_OnClosed() {
            // Clear user permissions
            _permissions.Clear();

            _isConnecting = false;
            context.IsConnected = false;
            //_heartbeatTimer.Enabled = false;
            _heartbeatTimer.Dispose();
            _connectionTimer.Dispose();
            _connectionTimerIsDisposed = true;

            if (_callDisconnectedCallback) {
                context.DelegateDisconnectedCallback();
                DoReconnect();
            }
        }

        private void _webSocketConnection_OnError(Exception error) {
            if (!_stopReconnecting) {
                if (_isConnecting) {
                    context.DelegateExceptionCallback(new OrtcGenericException(error.Message));

                    DoReconnect();
                } else {
                    context.DelegateExceptionCallback(new OrtcGenericException(String.Format("WebSocketConnection exception: {0}", error)));
                }
            }
        }

        private void sendValidateCommand() {
            _gotOnOpenCount++;
            if (_gotOnOpenCount > 1) {
                try {
                    //if (String.IsNullOrEmpty(ReadLocalStorage(_applicationKey, _sessionExpirationTime))) {
                    context.SessionId = Strings.GenerateId(16);
                    //}

                    string s;
                    if (context.HeartbeatActive) {
                        s = String.Format("validate;{0};{1};{2};{3};{4};{5};{6}", context._applicationKey, context._authenticationToken, context.AnnouncementSubChannel, context.SessionId,
                            context.ConnectionMetadata, context.HeartbeatTime, context.HeartbeatFails);
                    } else {
                        s = String.Format("validate;{0};{1};{2};{3};{4}", context._applicationKey, context._authenticationToken, context.AnnouncementSubChannel, context.SessionId, context.ConnectionMetadata);
                    }
                    DoSend(s);
                } catch (Exception ex) {
                    context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Exception sending validate: {0}", ex)));
                }
            }
        }

        private void  _webSocketConnection_OnMessageReceived(string message) {
            if (!String.IsNullOrEmpty(message)) {
                if (message[0] != 'c') {
                    if (_connectionTimerIsDisposed) {
                        _connectionTimer = new Timer(this._connectionTimer_Elapsed, null, Constants.SERVER_HB_COUNT * 1000, Constants.SERVER_HB_COUNT);
                        _connectionTimerIsDisposed = false;
                    } else {
                        _connectionTimer.Change(Constants.SERVER_HB_COUNT * 1000, Constants.SERVER_HB_COUNT * 1000);
                    }
                }

                // Open
                if (message == "o") {
                    sendValidateCommand();
                }
                    // Heartbeat
                else if (message == "h") {
                    // Do nothing
                } else {
                    message = message.Replace("\\\"", @"""");

                    // Update last keep alive time
                    _lastKeepAlive = DateTime.Now;

                    // Operation
                    Match operationMatch = Regex.Match(message, Constants.OPERATION_PATTERN);

                    if (operationMatch.Success) {
                        string operation = operationMatch.Groups["op"].Value;
                        string arguments = operationMatch.Groups["args"].Value;

                        switch (operation) {
                            case "ortc-validated":
                                _msgProcessor.ProcessOperationValidated(arguments);
                                break;
                            case "ortc-subscribed":
                                _msgProcessor.ProcessOperationSubscribed(arguments);
                                break;
                            case "ortc-unsubscribed":
                                _msgProcessor.ProcessOperationUnsubscribed(arguments);
                                break;
                            case "ortc-error":
                                _msgProcessor.ProcessOperationError(arguments);
                                break;
                            default:
                                // Unknown operation
                                context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Unknown operation \"{0}\" for the message \"{1}\"", operation, message)));

                                DoDisconnect();
                                break;
                        }
                    } else {
                        // Close
                        Match closeOperationMatch = Regex.Match(message, Constants.CLOSE_PATTERN);

                        if (!closeOperationMatch.Success) {
                            _msgProcessor.ProcessOperationReceived(message);
                        }
                    }
                }
            }

        }


        internal async void DoConnect() {
            _isConnecting = true;
            _callDisconnectedCallback = false;
            _gotOnOpenCount = 0;

            if (_webSocketConnection != null)
                _webSocketConnection.Dispose();

            _webSocketConnection = new Connection(_reconnectTimer);
            _webSocketConnection.OnOpened += new Connection.onOpenedDelegate(_webSocketConnection_OnOpened);
            _webSocketConnection.OnClosed += new Connection.onClosedDelegate(_webSocketConnection_OnClosed);
            _webSocketConnection.OnError += new Connection.onErrorDelegate(_webSocketConnection_OnError);
            _webSocketConnection.OnMessageReceived += new Connection.onMessageReceivedDelegate(_webSocketConnection_OnMessageReceived);

            if (context.IsCluster) {
                try {
                    context.Url = await Balancer.ResolveClusterUrlAsync(context.ClusterUrl); //"nope";// GetUrlFromCluster();

                    context.IsCluster = true;

                    if (String.IsNullOrEmpty(context.Url)) {
                        context.DelegateExceptionCallback(new OrtcEmptyFieldException("Unable to get URL from cluster"));
                        DoReconnect();
                    }
                } catch (Exception ex) {
                    if (!_stopReconnecting) {
                        context.DelegateExceptionCallback(new OrtcNotConnectedException(ex.Message));
                        DoReconnect();
                    }
                }
            }

            if (!String.IsNullOrEmpty(context.Url)) {
                try {
                    _webSocketConnection.Connect(context.Url);

                    // Just in case the server does not respond
                    //
                    _waitingServerResponse = true;

                    //StartReconnectTimer();
                    //
                } catch (OrtcEmptyFieldException ex) {
                    context.DelegateExceptionCallback(new OrtcNotConnectedException(ex.Message));
                    DoStopReconnecting();
                } catch (Exception ex) {
                    context.DelegateExceptionCallback(new OrtcNotConnectedException(ex.Message));
                    _isConnecting = false;
                }
            }

        }



        internal void DoReconnect() {
            if (!_stopReconnecting && !context.IsConnected) {
                if (_reconnectStartedAt != null) {
                    StartReconnectTimer();
                } else {
                    _reconnectStartedAt = DateTime.Now;

                    context.DelegateReconnectingCallback();

                    DoConnect();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        internal void DoStopReconnecting() {
            _isConnecting = false;
            _alreadyConnectedFirstTime = false;

            // Stop the connecting/reconnecting process
            _stopReconnecting = true;

            _reconnectStartedAt = null;

            if (_reconnectTimer.IsActive() /*!= null*/) {
                //_reconnectTimer.Stop();
                _reconnectTimer.Dispose();
            }
        }

        /// <summary>
        /// Disconnect the TCP client.
        /// </summary>
        internal void DoDisconnect() {
            _reconnectStartedAt = null;
            //_heartbeatTimer.Enabled = false;
            _heartbeatTimer.Dispose();
            _connectionTimer.Dispose();
            _connectionTimerIsDisposed = true;
            try {
                _webSocketConnection.Close();
            } catch (Exception ex) {
                context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Error disconnecting: {0}", ex)));
            }
        }


        internal void Send(string channel, string message) {
            byte[] channelBytes = Encoding.UTF8.GetBytes(channel);

            var domainChannelCharacterIndex = channel.IndexOf(':');
            var channelToValidate = channel;

            if (domainChannelCharacterIndex > 0) {
                channelToValidate = channel.Substring(0, domainChannelCharacterIndex + 1) + "*";
            }

            string hash = _permissions.Where(c => c.Key == channel || c.Key == channelToValidate).FirstOrDefault().Value;

            if (_permissions != null && _permissions.Count > 0 && String.IsNullOrEmpty(hash)) {
                context.DelegateExceptionCallback(new OrtcNotConnectedException(String.Format("No permission found to send to the channel '{0}'", channel)));
            } else {
                message = message.Replace(Environment.NewLine, "\n");

                message = Windows.Data.Json.JsonValue.CreateStringValue(message).Stringify();
                //message = Windows.Data.Json.JsonValue.CreateStringValue(message.Substring(1, message.Length-2)).Stringify();
                message = message.Substring(1, message.Length - 2);
                System.Diagnostics.Debug.WriteLine("s:: " + message);

                if (channel != String.Empty && message != String.Empty) {
                    try {
                        byte[] messageBytes = Encoding.UTF8.GetBytes(message);
                        List<String> messageParts = new List<String>();
                        int pos = 0;
                        int remaining;
                        string messageId = Strings.GenerateId(8);

                        // Multi part
                        while ((remaining = messageBytes.Length - pos) > 0) {
                            byte[] messagePart;

                            if (remaining >= Constants.MAX_MESSAGE_SIZE - channelBytes.Length) {
                                messagePart = new byte[Constants.MAX_MESSAGE_SIZE - channelBytes.Length];
                            } else {
                                messagePart = new byte[remaining];
                            }

                            Array.Copy(messageBytes, pos, messagePart, 0, messagePart.Length);

                            messageParts.Add(Encoding.UTF8.GetString((byte[])messagePart, 0, messagePart.Length));

                            pos += messagePart.Length;
                        }

                        for (int i = 0; i < messageParts.Count; i++) {
                            string s = String.Format("send;{0};{1};{2};{3};{4}", context._applicationKey, context._authenticationToken, channel, hash, String.Format("{0}_{1}-{2}_{3}", messageId, i + 1, messageParts.Count, messageParts[i]));

                            DoSend(s);
                        }
                    } catch (Exception ex) {
                        string exName = null;

                        if (ex.InnerException != null) {
                            exName = ex.InnerException.GetType().Name;
                        }

                        switch (exName) {
                            case "OrtcNotConnectedException":
                                // Server went down
                                if (context.IsConnected) {
                                    DoDisconnect();
                                }
                                break;
                            default:
                                context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Unable to send: {0}", ex)));
                                break;
                        }
                    }

                }
            }
        }

        /// <summary>
        /// Sends a message through the TCP client.
        /// </summary>
        /// <param name="message">The message to be sent.</param>
        internal void DoSend(string message) {
            try {
                _webSocketConnection.Send(message);
            } catch (Exception ex) {
                context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Unable to send: {0}", ex)));
            }
        }

        private void StartReconnectTimer() {
            if (_reconnectTimer.IsActive() /* != null*/) {
                //_reconnectTimer.Stop();
                _reconnectTimer.Dispose();
            }

            TimerCallback recCb = this._reconnectTimer_Elapsed;
            //_reconnectTimer = new Timer(recCb, null, context.ConnectionTimeout, context.ConnectionTimeout);
            _reconnectTimer.Start(recCb, context.ConnectionTimeout);

            /*
            _reconnectTimer.AutoReset = false;
            _reconnectTimer.Elapsed += new ElapsedEventHandler(_reconnectTimer_Elapsed);
            _reconnectTimer.Interval = ConnectionTimeout;
            _reconnectTimer.Start();
             */
        }


        private void _reconnectTimer_Elapsed(object sender) {
            if (!_stopReconnecting && !context.IsConnected) {
                if (_waitingServerResponse) {
                    _waitingServerResponse = false;
                    context.DelegateExceptionCallback(new OrtcNotConnectedException("Unable to connect"));
                }

                _reconnectStartedAt = DateTime.Now;

                context.DelegateReconnectingCallback();

                DoConnect();
            }
        }

        private void _heartbeatTimer_Elapsed(object sender) {
            if (context.IsConnected) {
                DoSend("b");
            }
        }

        private void _connectionTimer_Elapsed(object sender) {
            //System.Diagnostics.Debug.WriteLine("Server HB failed!");
            _webSocketConnection_OnClosed();
        }

        internal void SendProxy(string applicationKey, string privateKey, string channel, string message) {
            message = message.Replace(Environment.NewLine, "\n");
            byte[] channelBytes = Encoding.UTF8.GetBytes(channel);


            if (channel != String.Empty && message != String.Empty) {
                try {
                    byte[] messageBytes = Encoding.UTF8.GetBytes(message);
                    List<String> messageParts = new List<String>();
                    int pos = 0;
                    int remaining;
                    string messageId = Strings.GenerateId(8);

                    // Multi part
                    while ((remaining = messageBytes.Length - pos) > 0) {
                        byte[] messagePart;

                        if (remaining >= Constants.MAX_MESSAGE_SIZE - channelBytes.Length) {
                            messagePart = new byte[Constants.MAX_MESSAGE_SIZE - channelBytes.Length];
                        } else {
                            messagePart = new byte[remaining];
                        }

                        Array.Copy(messageBytes, pos, messagePart, 0, messagePart.Length);

                        messageParts.Add(Encoding.UTF8.GetString((byte[])messagePart, 0, messagePart.Length));

                        pos += messagePart.Length;
                    }

                    for (int i = 0; i < messageParts.Count; i++) {
                        string s = String.Format("sendproxy;{0};{1};{2};{3}", applicationKey, privateKey, channel, String.Format("{0}_{1}-{2}_{3}", messageId, i + 1, messageParts.Count, messageParts[i]));

                        DoSend(s);
                    }
                } catch (Exception ex) {
                    string exName = null;

                    if (ex.InnerException != null) {
                        exName = ex.InnerException.GetType().Name;
                    }

                    switch (exName) {
                        case "OrtcNotConnectedException":
                            // Server went down
                            if (context.IsConnected) {
                                DoDisconnect();
                            }
                            break;
                        default:
                            context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Unable to send: {0}", ex)));
                            break;
                    }
                }
            }
                

        }

        internal void subscribe(string channel, bool subscribeOnReconnected, OrtcClient.OnMessageDelegate onMessage) {
            var domainChannelCharacterIndex = channel.IndexOf(':');
            var channelToValidate = channel;

            if (domainChannelCharacterIndex > 0) {
                channelToValidate = channel.Substring(0, domainChannelCharacterIndex + 1) + "*";
            }

            string hash = _permissions.Where(c => c.Key == channel || c.Key == channelToValidate).FirstOrDefault().Value;

            if (_permissions != null && _permissions.Count > 0 && String.IsNullOrEmpty(hash)) {
                context.DelegateExceptionCallback(new OrtcNotConnectedException(String.Format("No permission found to subscribe to the channel '{0}'", channel)));
            } else {
                if (!_subscribedChannels.ContainsKey(channel)) {
                    _subscribedChannels.TryAdd(channel,
                        new ChannelSubscription {
                            IsSubscribing = true,
                            IsSubscribed = false,
                            SubscribeOnReconnected = subscribeOnReconnected,
                            OnMessage = onMessage
                        });
                }

                try {
                    if (_subscribedChannels.ContainsKey(channel)) {
                        ChannelSubscription channelSubscription = null;
                        _subscribedChannels.TryGetValue(channel, out channelSubscription);

                        channelSubscription.IsSubscribing = true;
                        channelSubscription.IsSubscribed = false;
                        channelSubscription.SubscribeOnReconnected = subscribeOnReconnected;
                        channelSubscription.OnMessage = onMessage;
                    }

                    string s = String.Format("subscribe;{0};{1};{2};{3}", context._applicationKey, context._authenticationToken, channel, hash);

                    DoSend(s);
                } catch (Exception ex) {
                    string exName = null;

                    if (ex.InnerException != null) {
                        exName = ex.InnerException.GetType().Name;
                    }

                    switch (exName) {
                        case "OrtcNotConnectedException":
                            // Server went down
                            if (context.IsConnected) {
                                DoDisconnect();
                            }
                            break;
                        default:
                            context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Unable to subscribe: {0}", ex)));
                            break;
                    }
                }
            }
        }








        internal void unsubscribe(string channel) {
            try {
                string s = String.Format("unsubscribe;{0};{1}", context._applicationKey, channel);

                DoSend(s);
            } catch (Exception ex) {
                string exName = null;

                if (ex.InnerException != null) {
                    exName = ex.InnerException.GetType().Name;
                }

                switch (exName) {
                    case "OrtcNotConnectedException":
                        // Server went down
                        if (context.IsConnected) {
                            DoDisconnect();
                        }
                        break;
                    default:
                        context.DelegateExceptionCallback(new OrtcGenericException(String.Format("Unable to unsubscribe: {0}", ex)));
                        break;
                }
            }
        }

        internal void startHeartbeat() {
            if (_heartbeatTimer == null) {
                _heartbeatTimer = new Timer(this._heartbeatTimer_Elapsed, null, Timeout.Infinite, context.HeartbeatTime);
            }
            _heartbeatTimer.Change(0, context.HeartbeatTime * 1000);
        }
    }
}
