require_relative 'book'
require_relative 'local_cache'
require 'dalli'
require 'json'

  class DataAccess 
  
    def initialize database, cache  
       @database = database 
       @Remote_cache = cache   
       @local_cache = LocalCache.new
    end
    
    def startUp 
    	 @database.startUp 
    end

    def shutDown
      @database.shutDown
    end

    def isbnSearch isbn
      result = nil
      local_copy = @local_cache.get isbn
      unless local_copy
          memcache_version = @Remote_cache.get "v_#{isbn}"
          if memcache_version
             memcache_copy = @Remote_cache.get "#{isbn}_#{memcache_version}" 
             result = Book.from_cache memcache_copy
             @local_cache.set result.isbn, {book: result, version: memcache_version.to_i}       
          else 
             result = @database.isbnSearch isbn
             if result
                @Remote_cache.set "v_#{result.isbn}", 1
                @Remote_cache.set "#{result.isbn}_1", result.to_cache
                @local_cache.set result.isbn, {book: result, version: 1}
             end
          end
      else
          memcache_version = @Remote_cache.get "v_#{isbn}"
          if memcache_version.to_i == local_copy[:version]
             result = local_copy[:book]
          else 
             memcache_copy = @Remote_cache.get "#{isbn}_#{memcache_version}" 
             result = Book.from_cache memcache_copy
             @local_cache.set result.isbn, {book: result, version: memcache_version.to_i}       
          end
      end
      result
    end

    def authorSearch author
        result = nil
        memcached_isbns = @Remote_cache.get "bks_#{author}"
        if memcached_isbns
           isbn_array = memcached_isbns.split(',')
           complex_object_key_parts = isbn_array.map do |isbn|
              buildISBNVersionString isbn, nil
           end
           key = "#{author}_#{complex_object_key_parts.join('_')}"
           value = @Remote_cache.get key
           if value
              result = JSON.parse value
           else
              books = complex_object_key_parts.map do |element| 
                 Book.from_cache(@Remote_cache.get element)
              end
              result = computeAuthorReport books
              @Remote_cache.set key,result.to_json
           end
        else
          books = @database.authorSearch author
          @Remote_cache.set "bks_#{author}", 
                         (books.map{|book| book.isbn }).join(',')
          complex_object_key_parts = books.map do |book|
               buildISBNVersionString book.isbn, book
          end
          key = "#{author}_#{complex_object_key_parts.join('_')}"
          result = computeAuthorReport books
          @Remote_cache.set key,result.to_json
        end
        result
    end

    def updateBook book
      @database.updateBook book
      remote_version = @Remote_cache.get "v_#{book.isbn}"
      if remote_version
         new_version = remote_version.to_i + 1
         @Remote_cache.set "v_#{book.isbn}", new_version
         @Remote_cache.set "#{book.isbn}_#{new_version}", book.to_cache
         if @local_cache.get book.isbn
            @local_cache.set book.isbn,  {book: book, version: new_version}
         end
      end
    end

private

   def computeAuthorReport books
       result = { }
       result['books'] = 
             books.collect {|book| {'title' => book.title, 'isbn' => book.isbn } }
        result['value'] = 
             books.inject(0) {|value,book| value += book.quantity * book.price }
        result
    end

    def buildISBNVersionString isbn, book
          isbn_version = @Remote_cache.get  "v_#{isbn}"
          if isbn_version
             "#{isbn}_#{isbn_version}"
          else
             @Remote_cache.set "v_#{isbn}", 1
             (book = @database.isbnSearch isbn) unless book 
             @Remote_cache.set "#{isbn}_1", book.to_cache
             "#{isbn}_1"
          end
    end

end 
