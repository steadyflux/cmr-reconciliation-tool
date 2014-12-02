class FieldStat
  def initialize(field_name, echo_xpath, dif_xpath, verbose = false)
    @field_name = field_name
    @echo_xpath = echo_xpath
    @dif_xpath = dif_xpath
    @verbose = verbose
    @total_stats = 0
    @total_match = 0
    @total_missing_dif = 0
    @total_missing_entry_id = 0
  end   
  
  def xml_compare(dif, echo)
    dif_value = (dif == "MISSING" || dif == "NO ENTRY ID") ? dif : XpathHelpers.find_xpath(@dif_xpath, dif).text
    echo_value = XpathHelpers.find_xpath(@echo_xpath, echo).text
    addStat(echo_value, dif_value)

    if @verbose
      puts "\t#{@field_name} (Match = #{XpathHelpers.isEqual?(dif_value, echo_value)})"
      puts "---------------------------------------------------------------"
      if dif == "MISSING"
        puts "\t#{XpathHelpers.getDIFEntryID(echo)} MISSING ****************" 
      else
        puts "\t#{@dif_xpath} | #{dif_value[0..80]}" 
      end
      puts "\t#{@echo_xpath} | #{echo_value[0..80]}"
      puts
    end
  end  

  def addStat(echo_value, dif_value)
    @total_stats += 1
    if XpathHelpers.isEqual?(dif_value, echo_value)
      @total_match += 1
    end
    if (dif_value == "MISSING")
      @total_missing_dif += 1
    end
    if (dif_value == "NO ENTRY ID")
      @total_missing_entry_id += 1
    end
  end

  def printStats
    output = ""
    output << "Stats for '#{@field_name}':\n"
    output << "\tTotal Count: #{@total_stats}\n"
    output << "\tTotal Match: #{@total_match}\n"
    output << "\tTotal Missing: #{@total_missing_dif}\n"
    output << "\tTotal without Entry ID: #{@total_missing_entry_id}\n"
    output << "\tPercent Matching: #{@total_match.to_f/@total_stats.to_f*100}%\n"
  end

  def statCSV
    "#{@field_name},#{@total_stats},#{@total_match},#{@total_missing_dif},#{@total_missing_entry_id},#{@total_match.to_f/@total_stats.to_f*100}\n"
  end

end