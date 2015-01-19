/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function() {
  "use strict";

  // The chunk size of tailing the files, i.e., how many bytes will be shown
  // in the preview.
  var TAIL_CHUNK_SIZE = 32768;

  //This stores the current directory which is being browsed
  var current_directory = "";
  //This stores the absolute file path that has been opened in various modals
  var absolute_file_path = "";
  //This stores the file / directory name that has been opened in various modals
  var inode_name = "";

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  $(window).bind('hashchange', function () {
    $('#alert-panel').hide();

    var dir = window.location.hash.slice(1);
    if(dir == "") {
      dir = "/";
    }
    if(current_directory != dir) {
      browse_directory(dir);
    }
  });

  function network_error_handler(url) {
    return function (jqxhr, text, err) {
      switch(jqxhr.status) {
        case 401:
          var msg = '<p>Authentication failed when trying to open ' + url + ': Unauthorized.</p>';
          break;
        case 403:
          if(jqxhr.responseJSON !== undefined && jqxhr.responseJSON.RemoteException !== undefined) {
            var msg = '<p>' + jqxhr.responseJSON.RemoteException.message + "</p>";
            break;
          }
          var msg = '<p>Permission denied when trying to open ' + url + ': ' + err + '</p>';
          break;
        case 404:
          var msg = '<p>Path does not exist on HDFS or WebHDFS is disabled.  Please check your path or enable WebHDFS</p>';
          break;
        default:
          var msg = '<p>Failed to retrieve data from ' + url + ': ' + err + '</p>';
        }
      show_err_msg(msg);
    };
  }

  function append_path(prefix, s) {
    var l = prefix.length;
    var p = l > 0 && prefix[l - 1] == '/' ? prefix.substring(0, l - 1) : prefix;
    return p + '/' + s;
  }

  function get_response(data, type) {
    return data[type] !== undefined ? data[type] : null;
  }

  function get_response_err_msg(data) {
    return data.RemoteException !== undefined ? data.RemoteException.message : "";
  }

  /* This method displays the modal to change replication count */
  function view_replication_details(repl) {
    $('#owner-info-title').text("Replication information - " + inode_name);

    $('#replication-count').val(repl);
    $('#set-replication-button').click(setReplication);
    $('#replication-info').modal();
  }

  /* This method uses WebHDFS to set the replication of a file */
  function setReplication() {
    var url = '/webhdfs/v1' + absolute_file_path + '?op=SETREPLICATION'
    + '&replication=' + $('#replication-count').val();

    $.ajax(url,
      { type: 'PUT',
        crossDomain: true
      }).done(function(data) {
        browse_directory(current_directory);
    }).error(network_error_handler(url)
     ).complete(function() {
       $('#replication-info').modal('hide');
       $('#set-replication-button').button('reset');
     });
  }

  // Use WebHDFS to set owner and group on an absolute path
  function setOwnerGroup(owner, group) {
    var url = '/webhdfs/v1' + absolute_file_path + '?op=SETOWNER'
    + '&owner=' + owner + '&group=' + group;

    $.ajax(url,
      { type: 'PUT',
        crossDomain: true
      }).error(network_error_handler(url)
     ).complete(function() {
       $('#owner-info').modal('hide');
       $('#set-owner-button').button('reset');
       browse_directory(current_directory);
     });
  }

  /* This method loads the checkboxes on the permission info modal. It accepts
   * the string representation, eg. '-rwxr-xr-t' or 'drwxrwx---' and infers the
   * checkboxes that should be true and false
   */
  function view_perm_details(perms) {
    $('#perm-info-title').text("Permissions information - " + inode_name);

    var arr = ["#sticky", "#perm-ur", "#perm-uw", "#perm-ux", "#perm-gr",
               "#perm-gw", "#perm-gx", "#perm-or", "#perm-ow", "#perm-ox"]

    //Sticky bit could be set on first char (dir) or last (file)
    $( arr[0] ).prop( 'checked',
      perms.charAt(0) == 't' || perms.charAt(9) == 't' );

    for(var i = 1; i < perms.length; i++) {
      $( arr[i] ).prop( 'checked', perms.charAt(i) != '-');
    }
    $('#set-perm-button').click(setPermissions);
    $('#perm-info').modal();
  }

  /* Figure out the Octal permissions from the checkboxes. This depends on
   * chmod to assume ommitted digits to be leading zeros.
   */
  function convertCheckboxesToOctalPermissions() {
    var p = 0
    p += $( '#sticky' ).prop( 'checked' ) ? 1000 : 0;
    p += $( '#perm-ur' ).prop( 'checked' ) ? 400 : 0;
    p += $( '#perm-uw' ).prop( 'checked' ) ? 200 : 0;
    p += $( '#perm-ux' ).prop( 'checked' ) ? 100 : 0;
    p += $( '#perm-gr' ).prop( 'checked' ) ? 40 : 0;
    p += $( '#perm-gw' ).prop( 'checked' ) ? 20 : 0;
    p += $( '#perm-gx' ).prop( 'checked' ) ? 10 : 0;
    p += $( '#perm-or' ).prop( 'checked' ) ? 4 : 0;
    p += $( '#perm-ow' ).prop( 'checked' ) ? 2 : 0;
    p += $( '#perm-ox' ).prop( 'checked' ) ? 1 : 0;
    return "" + p;
  }

  // Use WebHDFS to set permissions on an absolute path
  function setPermissions() {
    var permissionMask = convertCheckboxesToOctalPermissions();

    // PUT /webhdfs/v1/<path>?op=SETPERMISSION&permission=<permission>
    var url = '/webhdfs/v1' + absolute_file_path + '?op=SETPERMISSION'
        + '&permission=' + permissionMask;

    $.ajax(url,
      { type: 'PUT',
        crossDomain: true
      }).done(function(data) {
        browse_directory(current_directory);
    }).error(network_error_handler(url)
     ).complete(function() {
      $('#perm-info').modal('hide');
      $('#set-perm-button').button('reset');
    });
  }

  function view_file_details(path, abs_path) {
    function show_block_info(blocks) {
      var menus = $('#file-info-blockinfo-list');
      menus.empty();

      menus.data("blocks", blocks);
      menus.change(function() {
        var d = $(this).data('blocks')[$(this).val()];
        if (d === undefined) {
          return;
        }

        dust.render('block-info', d, function(err, out) {
          $('#file-info-blockinfo-body').html(out);
        });

      });
      for (var i = 0; i < blocks.length; ++i) {
        var item = $('<option value="' + i + '">Block ' + i + '</option>');
        menus.append(item);
      }
      menus.change();
    }

    var url = '/webhdfs/v1' + abs_path + '?op=GET_BLOCK_LOCATIONS';
    $.get(url).done(function(data) {
      var d = get_response(data, "LocatedBlocks");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      $('#file-info-tail').hide();
      $('#file-info-title').text("File information - " + path);

      var download_url = '/webhdfs/v1' + abs_path + '?op=OPEN';

      $('#file-info-download').attr('href', download_url);
      $('#file-info-preview').click(function() {
        var offset = d.fileLength - TAIL_CHUNK_SIZE;
        var url = offset > 0 ? download_url + '&offset=' + offset : download_url;
        $.get(url, function(t) {
          $('#file-info-preview-body').val(t);
          $('#file-info-tail').show();
        }, "text").error(network_error_handler(url));
      });

      if (d.fileLength > 0) {
        show_block_info(d.locatedBlocks);
        $('#file-info-blockinfo-panel').show();
      } else {
        $('#file-info-blockinfo-panel').hide();
      }
      $('#file-info').modal();
    }).error(network_error_handler(url));
  }

  function browse_directory(dir) {
    var url = '/webhdfs/v1' + dir + '?op=LISTSTATUS';
    $.get(url, function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      current_directory = dir;
      $('#directory').val(dir);
      window.location.hash = dir;
      dust.render('explorer', d, function(err, out) {
        $('#panel').html(out);

        $('.explorer-browse-links').click(function() {
          var type = $(this).attr('inode-type');
          var path = $(this).closest('tr').attr('inode-name');
          var abs_path = append_path(current_directory, path);
          if (type == 'DIRECTORY') {
            browse_directory(abs_path);
          } else {
            view_file_details(path, abs_path);
          }
        });

        //Set the handler for changing permissions
        $('.explorer-perm-links').click(function() {
          inode_name = $(this).closest('tr').attr('inode-name');
          absolute_file_path = append_path(current_directory, inode_name);
          var perms = $(this).html();
          view_perm_details(perms);
        });

        //Set the handler for changing owner/group
        $('.explorer-owner-links').blur(function() {
          var ownerOld = $(this).closest('tr').attr('inode-owner');
          var groupOld = $(this).closest('tr').attr('inode-group');
          var ownerNew = $(this).hasClass('owner')
                         ? $(this).html().trim() : ownerOld;
          var groupNew = $(this).hasClass('group')
                         ? $(this).html().trim() : groupOld;

          if( ownerOld != ownerNew || groupOld != groupNew) {
            //The owner/group was changed
            inode_name = $(this).closest('tr').attr('inode-name');
            absolute_file_path = append_path(current_directory, inode_name);
            setOwnerGroup(ownerNew, groupNew);
          }
        });
        //Disable line breaks in owner / group fields
        $('.explorer-owner-links').keypress(function(e){
          if(e.which == 13) {
            $('.explorer-owner-links').removeClass('editing');
            $('.explorer-owner-links').blur();
          }
        });

        //Set the handler for changing replication
        $('.explorer-replication-links').click(function() {
          inode_name = $(this).closest('tr').attr('inode-name');
          absolute_file_path = append_path(current_directory, inode_name);
          var repl = $(this).closest('tr').attr('inode-replication');
          view_replication_details(repl);
        });


      });
    }).error(network_error_handler(url));
  }


  function init() {
    dust.loadSource(dust.compile($('#tmpl-explorer').html(), 'explorer'));
    dust.loadSource(dust.compile($('#tmpl-block-info').html(), 'block-info'));

    var b = function() { browse_directory($('#directory').val()); };
    $('#btn-nav-directory').click(b);
    var dir = window.location.hash.slice(1);
    if(dir == "") {
      window.location.hash = "/";
    } else {
      browse_directory(dir);
    }
  }

  $('#upload-file').on('show.bs.modal', function(event) {
    var pwd = window.location.hash.slice(1);
    if( pwd.slice(-1) != '/' ) {
      pwd = pwd + '/';
    }

    var modal = $(this)
    $('#upload-file-button').on('click', function() {
      $(this).prop('disabled', true);
      $(this).button('complete');

      for(var i = 0; i < $('#upload-file-input').prop('files').length; i++) {
        var file = $('#upload-file-input').prop('files')[i];
        var url = '/webhdfs/v1' + pwd + file.name + '?op=CREATE';

        $.ajax({
          type: 'PUT',
          url: url,
          data: file,
          processData: false,
          crossDomain: true
        }).done(function(data) {
          browse_directory(pwd);
        }).error(network_error_handler(url));
        //TODO : Think about what happens when 1 / more files fail (modal gets hidden anyway)
      }
      modal.modal('hide');
      $('#upload-file-button').button('reset');
    });
  });


  $('#create-directory').on('show.bs.modal', function(event) {
    var modal = $(this)
    $('#new_directory_pwd').html(current_directory);
    $('#create-directory-button').on('click', function () {
      $(this).prop('disabled', true);
      $(this).button('complete');

      var url = '/webhdfs/v1' + append_path(current_directory,
        $('#new_directory').val()) + '?op=MKDIRS';

      $.ajax(url,
        { type: 'PUT',
          crossDomain: true
        }).done(function(data) {
          browse_directory(current_directory);
      }).error(network_error_handler(url)
       ).complete(function() {
         $('#create-directory').modal('hide');
         $('#create-directory-button').button('reset');
       });
    })
  });

  init();
})();
