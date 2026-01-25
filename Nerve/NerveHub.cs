using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nerve
{
    /// <summary>
    /// NerveHub adalah message broker in-memory yang ringan dan cepat.
    /// Dibuat oleh: Jacky the Code Bender.
    /// </summary>
    public class NerveHub
    {
        // Menyimpan mapping antara Topik dan List of Handlers.
        private readonly ConcurrentDictionary<string, List<HandlerWrapper>> _subscriptions = new();

        /// <summary>
        /// Delegate untuk membungkus handler agar generik.
        /// </summary>
        private class HandlerWrapper
        {
            public Guid Id { get; } = Guid.NewGuid();
            // Inisialisasi default agar tidak warning. Akan di-overwrite saat subscribe.
            public Func<object, Task> Action { get; set; } = _ => Task.CompletedTask;
        }

        /// <summary>
        /// Subscribe ke sebuah topik.
        /// </summary>
        public IDisposable Subscribe<T>(string topic, Func<T, Task> handler)
        {
            var wrapper = new HandlerWrapper
            {
                Action = async (obj) =>
                {
                    if (obj is T tObj)
                    {
                        await handler(tObj);
                    }
                }
            };

            _subscriptions.AddOrUpdate(topic, 
                _ => new List<HandlerWrapper> { wrapper },
                (key, existingList) => 
                {
                    lock (existingList)
                    {
                        existingList.Add(wrapper);
                    }
                    return existingList;
                });

            return new SubscriptionToken(() => Unsubscribe(topic, wrapper));
        }

        public IDisposable Subscribe<T>(string topic, Action<T> handler)
        {
            return Subscribe<T>(topic, (msg) => {
                handler(msg);
                return Task.CompletedTask;
            });
        }

        /// <summary>
        /// Publish pesan ke topik tertentu secara Asynchronous.
        /// </summary>
        public async Task PublishAsync<T>(string topic, T message)
        {
            // Pastikan message tidak null jika diperlukan, atau handle di wrapper. 
            // Di sini kita izinkan null jika T nullable, tapi C# warning system mungkin complain jika T non-nullable object.
            
            if (_subscriptions.TryGetValue(topic, out var handlers))
            {
                HandlerWrapper[] handlersSnapshot;
                
                lock (handlers) 
                {
                    handlersSnapshot = handlers.ToArray();
                }

                foreach (var handler in handlersSnapshot)
                {
                    try 
                    {
                        // Pass message as object (boxing happens here if struct)
                        await handler.Action(message!);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Nerve Error] Error on handling topic {topic}: {ex.Message}");
                    }
                }
            }
        }

        public void Publish<T>(string topic, T message)
        {
            _ = PublishAsync(topic, message);
        }

        private void Unsubscribe(string topic, HandlerWrapper wrapper)
        {
            if (_subscriptions.TryGetValue(topic, out var handlers))
            {
                lock (handlers)
                {
                    handlers.RemoveAll(x => x.Id == wrapper.Id);
                }
            }
        }

        private class SubscriptionToken : IDisposable
        {
            private readonly Action _unsubscribeAction;
            private bool _disposed;

            public SubscriptionToken(Action unsubscribeAction)
            {
                _unsubscribeAction = unsubscribeAction;
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _unsubscribeAction();
                    _disposed = true;
                }
            }
        }
    }
}
