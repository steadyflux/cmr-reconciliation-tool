#!/usr/bin/env ruby

require 'rubygems'
require 'nokogiri'
require 'sqlite3'
require 'commander/import'
require './fieldstat'
require './xpathHelpers'

program :version, '0.0.1'
program :description, 'Simple build_reconciliation_report utilities'
global_option '-V','--verbose','Enable verbose mode'

UMMC_FIELDS_TO_SCAN = {
  # "Metadata Revision Dates (Creation)" => FieldStat.new("Metadata Revision Dates (Creation)", "/Collection/InsertTime", "/DIF/DIF_Creation_Date"),
  # "Metadata Revision Dates (Revision)" => FieldStat.new("Metadata Revision Dates (Revision)", "/Collection/LastUpdate", "/DIF/Last_DIF_Revision_Date"),
  "Entry Title" => FieldStat.new("Entry Title", "/Collection/LongName", "/DIF/Entry_Title"),
  "Entry ID" => FieldStat.new("Entry ID", "/Collection/ShortName", "/DIF/Entry_ID", false),
  "Abstract" => FieldStat.new("Abstract", "/Collection/Description", "/DIF/Summary/Abstract"),
  "Purpose" => FieldStat.new("Purpose", "/Collection/SuggestedUsage", "/DIF/Summary/Purpose"),
  # "Organization (Archive)" => FieldStat.new("Organization (Archive)", "/Collection/ArchiveCenter", "/DIF/Data_Center/Data_Center_Name/Short_Name"),
  # "Organization (Processing Center)" => FieldStat.new("Organization (Processing Center)", "/Collection/ProcessingCenter", "/DIF/Data_Center/Data_Center_Name/Short_Name"),
  "Related URL (Access)" => FieldStat.new("Related URL (Access)", "/Collection/OnlineAccessURLs/OnlineAccessURL", "/DIF/Related_URL"),
  "Related URL (Resource)" => FieldStat.new("Related URL (Resource)", "/Collection/OnlineResources/OnlineResource", "/DIF/Related_URL")

  # "Metadata Revision Dates (Creation)" => {"ECHO" => "/Collection/InsertTime", "DIF" => "/DIF/DIF_Creation_Date"},
  # "Metadata Revision Dates (Revision)" => {"ECHO" => "/Collection/LastUpdate", "DIF" => "/DIF/Last_DIF_Revision_Date"},
  # "Entry Title" => {"ECHO" => "/Collection/LongName", "DIF" => "/DIF/Entry_Title"},
  # "Entry ID" => {"ECHO" => "/Collection/ShortName", "DIF" => "/DIF/Entry_ID"},
  # "Abstract" => {"ECHO" => "/Collection/Description", "DIF" => "/DIF/Summary/Abstract"},
  # "Purpose" => {"ECHO" => "/Collection/SuggestedUsage", "DIF" => "/DIF/Summary/Purpose"},
  # "Organization" => {"ECHO" => "/Collection/ProcessingCenter", "DIF" => "/DIF/Data_Center/Data_Center_Name/Short_Name"},
  # "Organization2" => {"ECHO" => "/Collection/ArchiveCenter", "DIF" => "/DIF/Data_Center/Data_Center_Name/Short_Name"},
  # "Personnel" => {"ECHO" => "/Collection/Contacts/Contact", "DIF" => "/DIF/Personnel"},
  # "Related URL" => {"ECHO" => "/Collection/OnlineAccessURLs/OnlineAccessURL", "DIF" => "/DIF/Related_URL"},
  # "Related URL2"=> {"ECHO" => "/Collection/OnlineResources/OnlineResource", "DIF" => "/DIF/Related_URL"},
  # "Collection Citation" => {"ECHO" => "/Collection/CitationForExternalPublication", "DIF" => "/DIF/Data_Set_Citation"}
}

DIF_DB = SQLite3::Database.new('dif_records.db')
ECHO_DB = SQLite3::Database.new('echo_collections.db')



def get_dif(entry_id)
  if entry_id.empty?
    return "NO ENTRY ID"
  end
  full_entry_id = (/^\[.*\].*$/ =~ entry_id) ? entry_id : "[%]#{entry_id}"
  dif_record = DIF_DB.execute("select * from difs WHERE entry_id LIKE \'#{full_entry_id}\'")[0]
  (dif_record == nil) ? "MISSING" : dif_record[1]
end

def get_echo_from_entry_id(entry_id, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE associated_dif LIKE \"#{entry_id}\"")[0] # and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? "MISSING" : [echo_record[3], echo_record[0]] 
end

def guess_echo_from_short_name(entry_id, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE short_name LIKE \"#{entry_id}\"")[0] #  and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? nil : [echo_record[3], echo_record[0]]
end

def guess_echo_from_short_name_version(entry_id, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE short_name LIKE \"#{entry_id.chop}\"")[0] #  and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? nil : [echo_record[3], echo_record[0]]
end

def guess_echo_from_datasetID(entry_title, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE datasetID LIKE \"%#{entry_title.strip}%\"")[0] #  and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? nil : [echo_record[3], echo_record[0]]
end

def guess_echo_from_long_name(entry_title, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE long_name LIKE \"%#{entry_title.strip}%\"")[0] #  and collection_id LIKE \'%#{provider}%\'")[0]
puts "#{echo_record[0]}" unless echo_record == nil
  (echo_record == nil) ? nil : [echo_record[3], echo_record[0]]
end

def build_reconciliation_report statement, outfile=nil, verbose=false
  missing = []
  no_entry_id = []
  matches = []
  ECHO_DB.execute(statement) do |row|
    echo = row[3]
    shortName = XpathHelpers.find_xpath("/Collection/ShortName", echo).text
    longName = XpathHelpers.find_xpath("/Collection/LongName", echo).text
    version = XpathHelpers.find_xpath("/Collection/VersionId", echo).text
    entry_id = XpathHelpers.find_xpath("/Collection/AssociatedDIFs/DIF/EntryId", echo).text
    last_update = XpathHelpers.find_xpath("/Collection/LastUpdate", echo).text
    
    puts "#{row[0]}^#{shortName}^#{version}^#{longName}^#{entry_id}^#{last_update}" if verbose
    dif = get_dif(entry_id)

    # no_entry_id << "#{entry_id}^#{shortName}^#{version}^#{last_update}" if dif == "NO ENTRY ID"
    # missing << "#{entry_id}^#{shortName}^#{version}^#{last_update}" if dif == "MISSING"
    # matches << "#{entry_id}^#{shortName}^#{version}^#{last_update}" unless (dif == "MISSING" || dif == "NO ENTRY ID")
      
    UMMC_FIELDS_TO_SCAN.each do |key, value|
      value.xml_compare(dif, echo)
    end

    # if verbose
    #   3.times { puts ":::"}
    #   puts echo
    #   3.times { puts }
    #   puts dif
    # end
  end
  output = ""
  UMMC_FIELDS_TO_SCAN.each do |key, value|
    # output << value.printStats
    output << value.statCSV
  end
  if outfile
    File.open(outfile, "w") { |aFile| aFile.puts output }
  else
    puts output
    puts "matches #{matches.length}" 
    matches.map {|a| puts a } if verbose
    puts "missing #{missing.length}" 
    missing.map {|a| puts a } if verbose
    puts "no_entry_id #{no_entry_id.length}" 
    no_entry_id.map {|a| puts a } if verbose
  end
end

def build_dif_reconciliation_report statement, gcmd_provider=nil, echo_provider=nil, outfile=nil, verbose=false
  count = 0
  found = []
  missing = []
  shortNameGuess = []
  datasetIDGuess = []
  shortNameVersionGuess = []
  longNameGuess = []
  really_missing = []
  DIF_DB.execute(statement) do |row|
    dif = row[1]
    # puts provider
    if gcmd_provider
      XpathHelpers.find_xpath("/DIF/Data_Center[1]/Data_Center_Name/Short_Name", dif).to_a.map do |a|
        if a.text.include? gcmd_provider
          entry_id = row[0][6..-1]
          # puts entry_id
          entry_title = XpathHelpers.find_xpath("/DIF/Entry_Title", row[1]).text
          last_update = XpathHelpers.find_xpath("/DIF/Last_DIF_Revision_Date", row[1]).text
          count += 1
          echo, id = get_echo_from_entry_id(entry_id, echo_provider)
          if echo == "MISSING"
            missing << "#{entry_id}"
            echo, id = guess_echo_from_short_name(entry_id, echo_provider)
            if echo
              shortNameGuess << "#{entry_id}^#{XpathHelpers.find_xpath("/Collection/ShortName", echo).text}^#{id}^#{last_update}"
            else
              echo, id = guess_echo_from_short_name_version(entry_id, echo_provider)
              if echo
                shortNameVersionGuess << "#{entry_id}^#{XpathHelpers.find_xpath("/Collection/ShortName", echo).text}^#{XpathHelpers.find_xpath("/Collection/VersionId", echo).text}^#{id}^#{last_update}"
              else
                echo, id = guess_echo_from_datasetID(entry_title, echo_provider)
                if echo
                  datasetIDGuess << "#{entry_id}^#{XpathHelpers.find_xpath("/Collection/DataSetId", echo).text}^#{id}"
                else
                  echo, id = guess_echo_from_long_name(entry_title, echo_provider)
                  if echo
                    longNameGuess << "#{entry_id}^#{XpathHelpers.find_xpath("/Collection/LongName", echo).text}^#{id}"
                  else
                    really_missing << "#{entry_id}^#{entry_title}^#{last_update}"
                  end
                end
              end
            end
          else
            found << "#{entry_id}^#{id}^#{last_update}"
          end
        end
      end
    end
  end
  
  puts "found:"
  found.map {|a| puts a }
  puts "missing:" 
  missing.map {|a| puts a }
  3.times {puts ":::"} 
  puts "short name:"
  shortNameGuess.map {|a| puts a}
  puts "datasetID:"
  datasetIDGuess.map {|a| puts a }
  puts "short name + version"
  shortNameVersionGuess.map {|a| puts a}
  puts "long_name"
  longNameGuess.map {|a| puts a}
  puts "really_missing"
  really_missing.map {|a| puts a}
  puts "Total GCMD Entries: #{count}"
  puts "#{missing.length} missing ECHO associated mappings."
  puts "#{shortNameGuess.length} potential matches based on shortName Guesses."
  puts "#{datasetIDGuess.length} potential matches based on datasetID Guesses."
  puts "#{shortNameVersionGuess.length} potential matches based on shortName + version Guesses."
  puts "#{longNameGuess.length} potential matches based on longName Guesses."
  puts "#{really_missing.length} really missing."
end

def reconcile_single_echo_record collection_id, verbose=false
  build_reconciliation_report "select * from collections where collection_id LIKE \'#{collection_id}'", nil, verbose
end

def reconcile_full_echo_provider provider, verbose=false
  build_reconciliation_report "select * from collections where provider = '#{provider}'", nil, verbose
end

def reconcile_single_dif_record entry_id, verbose=false
  build_dif_reconciliation_report "select * from difs where entry_id LIKE \'%#{entry_id}%'", nil, verbose
end

def reconcile_full_dif_provider gcmd_provider, echo_provider, verbose=false
  build_dif_reconciliation_report "select * from difs", gcmd_provider, echo_provider, nil, verbose
end

command :recon_report_from_echo do |c|
  c.action do |args, options|
    reconcile_full_echo_provider args[0], options.verbose
  end
end

command :recon_echo_record do |c|
  c.action do |args, options|
    reconcile_single_echo_record args[0], options.verbose
  end
end

command :recon_dif_record do |c|
  c.action do |args, options|
    reconcile_single_dif_record args[0], options.verbose
  end
end

command :recon_report_from_dif do |c|
  c.action do |args, options|
    reconcile_full_dif_provider args[0], args[1], options.verbose
  end
end