
class Book      
  
  attr_reader   :isbn 
  attr_accessor :price, :title, :author, :category, :quantity
  
  def initialize(isbn, title, author, price, category, quantity)
    @isbn  = isbn
    @author = author
    @title = title
    @price = Float(price)
    @category = category
    @quantity = quantity.to_i
  end  
  
  def to_s
  	"#{@isbn} - #{@title} by #{@author}. #{@category}. Price #{@price} In-stock #{@quantity}"
  end

  def to_cache
     "#{@isbn},#{@title},#{@author},#{@category},#{@price},#{@quantity}"
  end

  def self.from_cache serialized
      fields = serialized.split(',')
      Book.new fields[0], fields[1], fields[2], 
                 fields[4], fields[3], fields[5]
  end

  def eql?(other)
    self.isbn == other.isbn && self.author == other.author &&
        self.title == other.title && self.category == other.category &&
        self.price == other.price && self.quantity == other.quantity
  end

end
