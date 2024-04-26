interface CacheValue {
  value: string;
  expireable: boolean;
  expiry: number;
  timestamp: number;
}

class CustomCache {
  cache: Record<string, CacheValue> = {};

  set(key: string, value: string, expireable: boolean, expiry: number) {
    let cacheValue: CacheValue = {
      value: value,
      expireable: expireable,
      expiry: expiry,
      timestamp: Date.now(),
    };
    this.cache[key] = cacheValue;
  }

  get(key: string) {
    if (this.cache[key]) {
      if (!this.cache[key].expireable) {
        return this.cache[key].value;
      } else {
        if (this.cache[key].expiry + this.cache[key].timestamp > Date.now()) {
          return this.cache[key].value;
        } else {
          delete this.cache[key];
          return null;
        }
      }
    } else {
      return null;
    }
  }
}

export { CustomCache };
