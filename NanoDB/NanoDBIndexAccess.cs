using System.Collections.Generic;

namespace domi1819.NanoDB
{
    public class NanoDBIndexAccess
    {
        private readonly Dictionary<string, int> dict;

        internal NanoDBIndexAccess(Dictionary<string, int> dict)
        {
            this.dict = dict;
        }

        public Dictionary<string, int>.KeyCollection GetAllIndexes()
        {
            return this.dict.Keys;
        }

        public int GetValue(string key)
        {
            return this.dict[key];
        }

        public bool ContainsKey(string key)
        {
            return this.dict.ContainsKey(key);
        }
    }
}
