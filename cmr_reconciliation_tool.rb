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
  "Metadata Revision Dates (Creation)" => FieldStat.new("Metadata Revision Dates (Creation)", "/Collection/InsertTime", "/DIF/DIF_Creation_Date"),
  "Metadata Revision Dates (Revision)" => FieldStat.new("Metadata Revision Dates (Revision)", "/Collection/LastUpdate", "/DIF/Last_DIF_Revision_Date"),
  "Entry Title" => FieldStat.new("Entry Title", "/Collection/LongName", "/DIF/Entry_Title"),
  "Entry ID" => FieldStat.new("Entry ID", "/Collection/ShortName", "/DIF/Entry_ID", false),
  "Abstract" => FieldStat.new("Abstract", "/Collection/Description", "/DIF/Summary/Abstract"),
  "Purpose" => FieldStat.new("Purpose", "/Collection/SuggestedUsage", "/DIF/Summary/Purpose"),
  "Organization" => FieldStat.new("Organization", "/Collection/ArchiveCenter", "/DIF/Data_Center/Data_Center_Name/Short_Name")
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
  dif_record = DIF_DB.execute("select * from dif_records WHERE entry_id LIKE \'#{full_entry_id}\'")[0]
  (dif_record == nil) ? "MISSING" : dif_record[2]
end

def get_echo_from_entry_id(entry_id, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE associated_dif LIKE \'#{entry_id}\' and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? "MISSING" : [echo_record[3], echo_record[0]] 
end

def guess_echo_from_short_name(entry_id, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE short_name LIKE \'#{entry_id}\' and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? nil : [echo_record[3], echo_record[0]]
end

def guess_echo_from_datasetID(entry_title, provider)
  echo_record = ECHO_DB.execute("select * from collections WHERE datasetID LIKE \'%#{entry_title}%\' and collection_id LIKE \'%#{provider}%\'")[0]
  (echo_record == nil) ? nil : [echo_record[3], echo_record[0]]
end

def build_reconciliation_report statement, outfile=nil, verbose=false
  missing = []
  ECHO_DB.execute(statement) do |row|
    echo = row[3]
    shortName = XpathHelpers.find_xpath("/Collection/ShortName", echo).text
    version = XpathHelpers.find_xpath("/Collection/VersionId", echo).text
    entry_id = XpathHelpers.find_xpath("/Collection/AssociatedDIFs/DIF/EntryId", echo).text
    
    # puts "#{row[0]} | #{shortName} | #{version} | #{entry_id}" 
    dif = get_dif(entry_id)

    if dif == "MISSING" || dif == "NO ENTRY ID"
      missing << "#{entry_id}, #{shortName}, #{version}"
    end
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
    puts "missing:" 
    missing.map {|a| puts a }
  end
end

def build_dif_reconciliation_report statement, provider=nil, outfile=nil, verbose=false
  count = 0
  missing = []
  shortNameGuess = []
  datasetIDGuess = []
  DIF_DB.execute(statement) do |row|
    dif = row[2]
    if provider
      XpathHelpers.find_xpath("/DIF/Data_Center/Data_Center_Name/Short_Name", dif).to_a.map do |a|
        if a.text.include? provider
          entry_id = row[0][6..-1]
          entry_title = XpathHelpers.find_xpath("/DIF/Entry_Title", row[2]).text
          count += 1
          echo, id = get_echo_from_entry_id(entry_id, "NSIDC")
          if echo == "MISSING"
            missing << "#{entry_id}"
            echo, id = guess_echo_from_short_name(entry_id, "NSIDC")
            if echo
              shortNameGuess << "#{XpathHelpers.find_xpath("/Collection/ShortName", echo).text}, #{id}"
            else
              echo, id = guess_echo_from_datasetID(entry_title, "NSIDC")
              if echo
                datasetIDGuess << "#{XpathHelpers.find_xpath("/Collection/DataSetId", echo).text}, #{id}"
              end
            end
          end
        end
      end
    end
  end
  
  puts "missing:" 
  missing.map {|a| puts a }
  3.times {puts ":::"} 
  puts "short name:"
  shortNameGuess.map {|a| puts a}
  puts "datasetID:"
  datasetIDGuess.map {|a| puts a }
  puts "Total GCMD Entries: #{count}"
  puts "#{missing.length} missing ECHO associated mappings."
  puts "#{shortNameGuess.length} potential matches based on shortName Guesses."
  puts "#{datasetIDGuess.length} potential matches based on datasetID Guesses."
end

def reconcile_single_echo_record collection_id, verbose=false
  build_reconciliation_report "select * from collections where collection_id LIKE \'#{collection_id}'", nil, verbose
end

def reconcile_full_echo_provider provider, verbose=false
  build_reconciliation_report "select * from collections where provider = '#{provider}'", nil, verbose
end

def reconcile_single_dif_record entry_id, verbose=false
  build_dif_reconciliation_report "select * from dif_records where entry_id LIKE \'#{entry_id}'", nil, verbose
end

def reconcile_full_dif_provider provider, verbose=false
  build_dif_reconciliation_report "select * from dif_records", provider, nil, verbose
end

command :recon_report do |c|
  c.action do |args, options|
    reconcile_full_echo_provider args[0]
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
    reconcile_full_dif_provider args[0], options.verbose
  end
end