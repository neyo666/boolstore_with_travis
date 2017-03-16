require_relative 'user_command'
class QuitCommand < UserCommand

	def initialize(data_source)
		super (data_source)
	end

	def title 
		'Quit.'
	end

   def input
   end

    def execute
       @data_source.shutDown
       puts 'Goodbye!'
	end

end