#!/usr/bin/env ruby

require 'rest_client'
require 'nokogiri'
require 'sqlite3'
require 'commander/import'

program :version, '0.0.1'
program :description, 'Dumb GCMD Link Checker'
global_option '-V','--verbose','Enable verbose mode'

BASE_URL = "http://gcmd.gsfc.nasa.gov/KeywordSearch/Metadata.do?Portal=GCMD&MetadataType=0&MetadataView=Full&KeywordPath=&EntryId="

command :check_links do |c|
  c.action do |args, options|
    broken = 0
    total = 0
    File.open("id_list").readlines.each do |line|
      if line.start_with?("---")
        puts "..............Halfway there resetting broken counter (total: #{total}, broken: #{broken}.............."
      else  
        if line.start_with?("!!")
          puts "QUITTIN' TIME"
          break
        else
          resource = RestClient::Resource.new(
            BASE_URL + line.strip,
            :timeout => nil
          )
          print line.strip
          begin
            response = resource.get
          rescue => e
            e.response
            broken += 1
            print ".....broken"
          end
          sleep(0.4)
          total += 1
          puts
        end 
      end
    end
  end
end